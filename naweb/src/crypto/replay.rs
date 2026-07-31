//! modern-v2 请求重放占位合同与单实例实现。

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

/// 重放存储返回的原子占位结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayDecision {
    /// rid 此前不存在，当前请求已经成功占位。
    Fresh,
    /// 同一安全域中的 rid 仍在保留期内。
    Duplicate,
}

/// 不携带后端地址、key 或 rid 的重放存储错误。
#[derive(Debug, Clone)]
pub struct ReplayError {
    /// 由实现写死的低基数原因，不得拼接后端响应和请求值。
    pub internal_reason: &'static str,
}

impl ReplayError {
    /// 建立可安全传给统一错误层的重放存储错误。
    ///
    /// # 参数
    ///
    /// - `internal_reason`: 受控内部原因，不得含地址、凭证或 rid。
    ///
    /// # 返回
    ///
    /// 返回后端不可用错误。
    pub const fn new(internal_reason: &'static str) -> Self {
        Self { internal_reason }
    }
}

impl std::fmt::Display for ReplayError {
    /// 输出固定错误说明，不泄露后端细节。
    ///
    /// # 参数
    ///
    /// - `formatter`: 错误链提供的输出目标。
    ///
    /// # 返回
    ///
    /// 返回标准格式化结果。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("重放存储不可用")
    }
}

impl std::error::Error for ReplayError {}

/// [`ReplayGuard`] 对象安全异步返回值。
pub type ReplayFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ReplayDecision, ReplayError>> + Send + 'a>>;

/// [`ReplayGuard`] 就绪探测使用的对象安全异步返回值。
///
/// 探测不接受请求域或凭证，只确认实现当前能否执行 required 路由所需的原子占位。
pub type ReplayReadinessFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), ReplayError>> + Send + 'a>>;

/// 一次重放占位使用的完整安全域键。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReplayKey {
    /// 已认证租户域，不得来自未认证 body。
    pub tenant_scope: Arc<str>,
    /// 路由静态声明的密钥域。
    pub key_scope: Arc<str>,
    /// 已在本地有限 key ring 中验证的公开 kid。
    pub kid: Arc<str>,
    /// 固定方向文本；第一版请求占位使用 `request`。
    pub direction: &'static str,
    /// 发起方生成并经 AEAD 认证的 16 字节请求标识。
    pub rid: [u8; 16],
}

/// 可替换的原子重放占位存储。
pub trait ReplayGuard: Send + Sync + 'static {
    /// 返回注册表使用的稳定 guard ID。
    ///
    /// # 返回
    ///
    /// 返回非空、有限长度的 ASCII 标识，不得包含实例地址。
    fn id(&self) -> &'static str;

    /// 原子创建带 TTL 的 rid 占位。
    ///
    /// # 参数
    ///
    /// - `key`: 已完成 AEAD 认证的完整重放域；在认证前绝不能调用本方法。
    /// - `ttl`: 占位保留时间，必须覆盖时钟窗口与最长请求时间。
    ///
    /// # 返回
    ///
    /// 仅 [`ReplayDecision::Fresh`] 允许继续；后端故障由 required 路由 fail closed。
    fn reserve<'a>(&'a self, key: ReplayKey, ttl: Duration) -> ReplayFuture<'a>;

    /// 探测 required 路由依赖的重放后端是否可用。
    ///
    /// # 返回
    ///
    /// 后端当前能执行原子占位时返回空值；连接、权限或服务故障返回受控错误。内存实现和无需
    /// 外部依赖的实现可使用默认成功结果，远程实现必须覆盖本方法。
    fn readiness<'a>(&'a self) -> ReplayReadinessFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}

/// 只适用于单进程部署和本地验证的有界内存重放存储。
pub struct InMemoryReplayGuard {
    id: &'static str,
    max_entries: usize,
    entries: Mutex<HashMap<ReplayKey, Instant>>,
}

impl InMemoryReplayGuard {
    /// 建立单进程有界重放存储。
    ///
    /// # 参数
    ///
    /// - `id`: 注册表稳定 ID，最大 64 字节。
    /// - `max_entries`: 保留期内最多保存的 rid 数量，满载时关闭新请求而非无界增长。
    ///
    /// # 返回
    ///
    /// 返回可注入运行时的共享 guard；非法容量返回错误。
    pub fn new(id: &'static str, max_entries: usize) -> Result<Self, ReplayError> {
        if id.is_empty() || id.len() > 64 || max_entries == 0 {
            return Err(ReplayError::new("memory-replay-config"));
        }
        Ok(Self {
            id,
            max_entries,
            entries: Mutex::new(HashMap::new()),
        })
    }
}

impl ReplayGuard for InMemoryReplayGuard {
    /// 返回构造时声明的稳定 ID。
    ///
    /// # 返回
    ///
    /// 返回注册表索引文本。
    fn id(&self) -> &'static str {
        self.id
    }

    /// 在单进程互斥表中执行原子检查与占位。
    ///
    /// # 参数
    ///
    /// - `key`: 已认证的完整重放域键，由值移动进存储。
    /// - `ttl`: 从当前单调时钟开始计算的保留时间。
    ///
    /// # 返回
    ///
    /// 新值返回 Fresh，存量未过期值返回 Duplicate；容量耗尽返回错误且不放行。
    fn reserve<'a>(&'a self, key: ReplayKey, ttl: Duration) -> ReplayFuture<'a> {
        Box::pin(async move {
            if ttl.is_zero() {
                return Err(ReplayError::new("memory-replay-zero-ttl"));
            }
            let now = Instant::now();
            let mut entries = self.entries.lock().await;
            entries.retain(|_, expires_at| *expires_at > now);
            if entries.contains_key(&key) {
                return Ok(ReplayDecision::Duplicate);
            }
            if entries.len() >= self.max_entries {
                return Err(ReplayError::new("memory-replay-capacity"));
            }
            entries.insert(key, now + ttl);
            Ok(ReplayDecision::Fresh)
        })
    }
}
