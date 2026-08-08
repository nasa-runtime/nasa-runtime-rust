//! 配置驱动的路由级隔离:yml 根 `grafana.isolation` → matchit Trie → 全局 dispatch 中间件。
//!
//! 实现 pattern 归一、context-path 剥离、MatchedPath 有界桶身份和 DashMap 懒建命令；
//! 规则入表统一走 [`crate::Command::monitor`] 的 0 值归一化(0 = 不限制/不超时,合同)。

use std::time::Duration;

use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;

use crate::command::{current_tps, Command};

/// 一条隔离模式的参数;`#[derive(Deserialize)]` 让业务 yml 直接反序列化。
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IsolationRule {
    /// 并发上限;缺省/0 = 不做 bulkhead(合同 归一化)。
    #[serde(default)]
    pub max_concurrent: usize,
    /// 执行超时毫秒;缺省/0 = 不做超时。
    #[serde(default)]
    pub timeout_ms: u64,
    /// TPS 权重;缺省 = 不计 TPS,显式 0 = 标记但权重 0。
    #[serde(default)]
    pub tps_weight: Option<u64>,
}

/// 运行期的一条隔离规则(原始参数,归一化统一发生在 Command::monitor)。
struct IsolationCfg {
    /// 原始模式(如 "/download/*"),MatchedPath 拿不到时回退作桶名。
    pattern: String,
    /// 并发上限(原始值,0 = 不限)。
    max_concurrent: usize,
    /// 超时毫秒(原始值,0 = 不超时)。
    timeout_ms: u64,
    /// TPS 权重标记。
    tps_weight: Option<u64>,
}

/// 全局隔离表:Trie + context-path 前缀 + 「路由 → 已建 Command」缓存。
struct IsolationTable {
    /// 前缀树:path → 隔离参数(matchit 与 axum 内部同款)。
    trie: matchit::Router<IsolationCfg>,
    /// context-path(如 "/order-service"),dispatch 匹配前从 path 剥掉。
    ctx_prefix: String,
    /// 路由 → 懒建命令缓存(热路径免走注册表全局锁;注册表按 key 去重兜底)。
    commands: dashmap::DashMap<String, std::sync::Arc<Command>>,
}

// 全局隔离表,启动时 init 一次;没配 grafana.isolation 则保持空(dispatch 全放行)。
static ISOLATION: std::sync::OnceLock<IsolationTable> = std::sync::OnceLock::new();

/// 业务作用：启动时调用一次:把 yml 的 `grafana.isolation` 建成 Trie。
/// 规则为空则不初始化 → [`dispatch`] 全部放行(= 配置驱动隔离未启用)。
///
/// # 参数
/// - `rules`: 「模式 → 规则」表,来自配置根 `grafana.isolation`。
/// - `context_path`: 服务 context-path;匹配前会从请求 path 剥掉。
pub fn init_isolation(
    rules: &std::collections::HashMap<String, IsolationRule>,
    context_path: &str,
) {
    if rules.is_empty() {
        return;
    }
    let mut trie = matchit::Router::new();
    let mut inserted = 0usize;
    for (pattern, rule) in rules {
        let route = normalize_pattern(pattern);
        let cfg = IsolationCfg {
            pattern: pattern.clone(),
            max_concurrent: rule.max_concurrent,
            timeout_ms: rule.timeout_ms,
            tps_weight: rule.tps_weight,
        };
        // 模式语法非法只告警跳过，不让整个启动失败。
        match trie.insert(route.clone(), cfg) {
            Ok(_) => inserted += 1,
            Err(e) => {
                tracing::warn!(
                    "grafana isolation pattern '{}' (route '{}') invalid, skipped: {}",
                    pattern,
                    route,
                    e
                );
            }
        }
    }
    if inserted == 0 {
        tracing::warn!("grafana isolation has no valid patterns, initialization skipped");
        return;
    }
    let ctx = context_path.trim().trim_end_matches('/').to_string();
    let table = IsolationTable {
        trie,
        ctx_prefix: ctx,
        commands: dashmap::DashMap::new(),
    };
    match ISOLATION.set(table) {
        Ok(()) => tracing::info!(
            "grafana config-driven isolation initialized ({} valid patterns)",
            inserted
        ),
        Err(_) => tracing::warn!(
            "grafana config-driven isolation is already initialized; new rules were ignored"
        ),
    }
}

/// 业务作用：把 "/download/*" 风格通配归一成 matchit 0.8 的命名 catch-all "/download/{*rest}"。
/// 非 "/*" 结尾的具体路由原样返回。
///
/// # 参数
/// - `pattern`: yml 里的路由模式。
fn normalize_pattern(pattern: &str) -> String {
    if let Some(prefix) = pattern.strip_suffix("/*") {
        format!("{prefix}/{{*rest}}")
    } else {
        pattern.to_string()
    }
}

/// 业务作用：仅在完整路径段边界上剥离 context-path，避免 `/app` 错误匹配 `/application`。
fn strip_context_path<'a>(path: &'a str, context_path: &str) -> &'a str {
    if context_path.is_empty() {
        return path;
    }
    if path == context_path {
        return "/";
    }
    match path.strip_prefix(context_path) {
        Some(rest) if rest.starts_with('/') => rest,
        _ => path,
    }
}

/// 业务作用：全局中间件:拿请求 path 在 Trie 上匹配,命中 → 按真实路由懒建/复用命令并套
/// bulkhead + 超时 + 双轨指标;未命中/未初始化 → 直接放行。
///
/// 用法:整个 app 套一层 `.layer(axum::middleware::from_fn(nasa::grafana::dispatch))`。
///
/// # 参数
/// - `req`: 本次进入全局中间件的 HTTP 请求。
/// - `next`: 未命中或执行通过时的 axum 放行句柄。
pub async fn dispatch(req: Request, next: Next) -> Response {
    let Some(table) = ISOLATION.get() else {
        return next.run(req).await;
    };
    let full = req.uri().path();
    let rel = strip_context_path(full, &table.ctx_prefix);
    match table.trie.at(rel) {
        Ok(m) => {
            let cfg = m.value;
            // 桶身份 = 本次命中的真实路由模板(MatchedPath,按 handler 有界,路径变量
            // 不拆桶);理论兜底:拿不到时退回配置模式(仍有界)。
            let route: String = match req.extensions().get::<axum::extract::MatchedPath>() {
                Some(mp) => {
                    let t = mp.as_str();
                    strip_context_path(t, &table.ctx_prefix).to_string()
                }
                None => cfg.pattern.clone(),
            };
            let cmd = table
                .commands
                .entry(route.clone())
                .or_insert_with(|| {
                    // 首次见到该路由才建命令(0 值归一化在 monitor 内统一发生);
                    // 同模式下各接口共用同一份参数值,各有独立的圈/信号量。
                    let c = Command::monitor(
                        &route,
                        "isolation",
                        Some(cfg.max_concurrent),
                        Some(Duration::from_millis(cfg.timeout_ms)),
                        cfg.tps_weight,
                    );
                    // 标了 TPS 的命令，周期请求日志行尾带实时 tps。
                    if cfg.tps_weight.is_some() {
                        c.set_extra(|_period_ms| format!("tps={:.0}/s", current_tps()));
                    }
                    c.set_path(&route);
                    c
                })
                .clone();
            cmd.run(req, next).await
        }
        Err(_) => next.run(req).await,
    }
}
