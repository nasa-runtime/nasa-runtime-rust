//! Prometheus 文本渲染与 `/metrics` handler。
//!
//! 自渲染文本格式(text/plain; version=0.0.4),不引第三方 metrics crate(先例:
//! gateway/src/metrics.rs)。渲染要求(合同):每族一次 HELP/TYPE、label 转义、
//! 输出按 (group, command) 排序稳定可 diff、抓取时 clone 注册表列表后立刻放锁。

use axum::http::header;
use axum::response::IntoResponse;

use crate::command::CommandExport;
use crate::counters::LATENCY_LE_LABELS;
use crate::registry;

/// `/metrics` handler:业务把它挂到路由即可被 Prometheus 抓取。
/// 是否鉴权由业务路由层自定(合同 安全边界);本组件不新增独立端口。
pub async fn metrics() -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        render_metrics(),
    )
}

/// 渲染全量指标文本。注册表快照(排序、放锁)→ 逐命令 export → 按族输出。
///
/// # 返回
///
/// 返回可与业务其它 Prometheus 指标安全拼接的 text exposition 片段。
pub fn render_metrics() -> String {
    // registry::all() 已按 (group, command) 排序;export 逐命令锁各自滚动窗口一次。
    let exports: Vec<CommandExport> = registry::all().iter().map(|c| c.export()).collect();
    let mut out = String::with_capacity(4096 + exports.len() * 2048);

    // ── 结局单调计数 ──
    out.push_str(
        "# HELP nafana_requests_total 接口请求结局单调计数(success/failure/timeout/rejected/canceled)。\n",
    );
    out.push_str("# TYPE nafana_requests_total counter\n");
    for e in &exports {
        for (outcome, v) in [
            ("success", e.success),
            ("failure", e.failure),
            ("timeout", e.timeout),
            ("rejected", e.rejected),
            ("canceled", e.canceled),
        ] {
            out.push_str(&format!(
                "nafana_requests_total{{command=\"{}\",group=\"{}\",outcome=\"{}\"}} {}\n",
                escape_label(&e.command),
                escape_label(&e.group),
                outcome,
                v
            ));
        }
    }

    // ── 降级/ TPS 单调计数 ──
    push_counter_family(
        &mut out,
        &exports,
        "nafana_fallback_total",
        "拒绝/超时分支产出降级响应的单调计数。",
        |e| e.fallback,
    );
    push_counter_family(
        &mut out,
        &exports,
        "nafana_tps_total",
        "TPS 单调计数:每请求(含被拒、被取消)按 tps_weight 累加;顶栏 TPS = sum(rate(...))。",
        |e| e.tps,
    );

    // ── 并发 gauges ──
    push_gauge_family(
        &mut out,
        &exports,
        "nafana_inflight",
        "当前执行区并发。",
        |e| e.inflight,
    );
    push_gauge_family(
        &mut out,
        &exports,
        "nafana_inflight_rolling_max",
        "10s 滚动窗口内并发峰值(随窗口回落)。",
        |e| e.rolling_max_inflight,
    );
    push_gauge_family(
        &mut out,
        &exports,
        "nafana_inflight_lifetime_max",
        "进程生命周期并发峰值(只增不减)。",
        |e| e.lifetime_max_inflight,
    );

    // ── 命令静态参数 numeric gauges(饱和度等 PromQL 数值计算的分母来源,合同) ──
    push_gauge_family(
        &mut out,
        &exports,
        "nafana_max_concurrent",
        "bulkhead 容量;0 = 不限并发。",
        |e| e.max_concurrent,
    );
    push_gauge_family(
        &mut out,
        &exports,
        "nafana_timeout_ms",
        "单请求超时毫秒;0 = 不超时。",
        |e| e.timeout_ms,
    );
    push_gauge_family(
        &mut out,
        &exports,
        "nafana_tps_weight",
        "TPS 权重;0 = 未标 TPS 或权重 0。",
        |e| e.tps_weight,
    );

    // ── 延迟直方图(跨实例聚合;bucket/sum/count 全部单调,合同) ──
    out.push_str(
        "# HELP nafana_latency_seconds 执行延迟直方图(秒);rejected/canceled 不进延迟统计。\n",
    );
    out.push_str("# TYPE nafana_latency_seconds histogram\n");
    for e in &exports {
        let c = escape_label(&e.command);
        let g = escape_label(&e.group);
        let h = &e.histogram;
        for (le, v) in LATENCY_LE_LABELS.iter().zip(h.cumulative.iter()) {
            out.push_str(&format!(
                "nafana_latency_seconds_bucket{{command=\"{c}\",group=\"{g}\",le=\"{le}\"}} {v}\n"
            ));
        }
        out.push_str(&format!(
            "nafana_latency_seconds_bucket{{command=\"{c}\",group=\"{g}\",le=\"+Inf\"}} {}\n",
            h.count
        ));
        out.push_str(&format!(
            "nafana_latency_seconds_sum{{command=\"{c}\",group=\"{g}\"}} {}\n",
            format_float(h.sum_seconds)
        ));
        out.push_str(&format!(
            "nafana_latency_seconds_count{{command=\"{c}\",group=\"{g}\"}} {}\n",
            h.count
        ));
    }

    // ── 展示元信息(不参与数值计算,合同) ──
    out.push_str("# HELP nafana_command_info 命令展示元信息(path = 真实路由)。\n");
    out.push_str("# TYPE nafana_command_info gauge\n");
    for e in &exports {
        out.push_str(&format!(
            "nafana_command_info{{command=\"{}\",group=\"{}\",path=\"{}\"}} 1\n",
            escape_label(&e.command),
            escape_label(&e.group),
            escape_label(&e.path)
        ));
    }

    out
}

/// 输出一族只按 (command, group) 打标签的 counter。
///
/// # 参数
/// - `out`: 渲染缓冲。
/// - `exports`: 全部命令导出视图。
/// - `name`: 指标名。
/// - `help`: HELP 文案。
/// - `pick`: 从导出视图取值的闭包。
fn push_counter_family(
    out: &mut String,
    exports: &[CommandExport],
    name: &str,
    help: &str,
    pick: impl Fn(&CommandExport) -> u64,
) {
    push_family(out, exports, name, help, "counter", pick);
}

/// 输出一族只按 (command, group) 打标签的 gauge。
///
/// # 参数
/// - `out`: 渲染缓冲。
/// - `exports`: 全部命令导出视图。
/// - `name`: 指标名。
/// - `help`: HELP 文案。
/// - `pick`: 从导出视图取值的闭包。
fn push_gauge_family(
    out: &mut String,
    exports: &[CommandExport],
    name: &str,
    help: &str,
    pick: impl Fn(&CommandExport) -> u64,
) {
    push_family(out, exports, name, help, "gauge", pick);
}

/// counter/gauge 族的公共输出逻辑。
///
/// # 参数
/// - `out`: 渲染缓冲。
/// - `exports`: 全部命令导出视图。
/// - `name`: 指标名。
/// - `help`: HELP 文案。
/// - `kind`: TYPE 行的类型字面量。
/// - `pick`: 从导出视图取值的闭包。
fn push_family(
    out: &mut String,
    exports: &[CommandExport],
    name: &str,
    help: &str,
    kind: &str,
    pick: impl Fn(&CommandExport) -> u64,
) {
    out.push_str(&format!("# HELP {name} {help}\n"));
    out.push_str(&format!("# TYPE {name} {kind}\n"));
    for e in exports {
        out.push_str(&format!(
            "{name}{{command=\"{}\",group=\"{}\"}} {}\n",
            escape_label(&e.command),
            escape_label(&e.group),
            pick(e)
        ));
    }
}

/// Prometheus label value 转义:`\` → `\\`、`"` → `\"`、换行 → `\n`(合同)。
///
/// # 参数
/// - `value`: 原始 label 值。
fn escape_label(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            other => escaped.push(other),
        }
    }
    escaped
}

/// 浮点渲染:整数值不带小数点尾巴、其余保留足够精度(sum 秒)。
///
/// # 参数
/// - `v`: 待渲染浮点值。
fn format_float(v: f64) -> String {
    if v == v.trunc() && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{v:.6}")
    }
}
