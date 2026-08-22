//! 非 Kafka connector 的消息级身份绑定。
//!
//! 生产 Kafka adapter 以 SASL principal、ACL 与冻结 topic route 解析逻辑 producer；HTTP/gRPC
//! 等自定义 connector 则必须在调用 authenticated runtime API 前提供等价证明。本模块给本地三服务
//! 门禁提供 HMAC-SHA-256 收据，避免把可由请求方任意填写的 producer header 当成认证结果。

use hmac::{Hmac, Mac as _};
use nasaga_core::ServiceIdentity;
use sha2::Sha256;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;

/// 业务作用：封装接收端实际观察到的签名字段，保证验签与 replay claim 消费同一份字节。
pub struct SagaHttpSignedMessage<'a> {
    producer: &'a ServiceIdentity,
    path: &'a str,
    timestamp_ms: u64,
    nonce: &'a str,
    body: &'a [u8],
    signature: &'a str,
}

impl<'a> SagaHttpSignedMessage<'a> {
    /// 业务作用：冻结一条待认证 HTTP 请求的 producer、route、时钟、nonce、body 与 HMAC。
    ///
    /// 参数说明：
    /// - `producer`: transport 凭据映射出的逻辑 producer。
    /// - `path`: 实际命中的固定 path。
    /// - `timestamp_ms`: 请求签名时间。
    /// - `nonce`: 请求的一次性随机数。
    /// - `body`: 收到的原始 body。
    /// - `signature`: 请求携带的 HMAC。
    ///
    /// 返回：只借用原始字段的不可变收据；编码与密码学检查由认证器执行。
    pub fn new(
        producer: &'a ServiceIdentity,
        path: &'a str,
        timestamp_ms: u64,
        nonce: &'a str,
        body: &'a [u8],
        signature: &'a str,
    ) -> Self {
        Self {
            producer,
            path,
            timestamp_ms,
            nonce,
            body,
            signature,
        }
    }
}

/// 业务作用：冻结单条 transport 信任边的 HMAC key 与允许时钟偏差。
///
/// 同一实例只能代表一个已由部署配置绑定的 producer。签名覆盖 producer、HTTP path、时间戳、
/// 一次性 nonce 和原始 body；接收方校验成功并占用 nonce 后，才能把该 producer 传给 runtime 的
/// authenticated API。
#[derive(Clone)]
pub struct SagaHttpMessageAuthenticator {
    template: HmacSha256,
    max_clock_skew_ms: u64,
}

impl SagaHttpMessageAuthenticator {
    /// 业务作用：从部署注入的 256 位十六进制 key 构造消息认证器。
    ///
    /// 参数说明：
    /// - `hex_key`: 恰好 64 个小写十六进制字符；不得写入日志、代码或 Outbox。
    /// - `max_clock_skew_ms`: 接收时间与签名时间允许的最大偏差，必须大于零。
    ///
    /// 返回：key 与时间窗口合法时返回认证器；否则拒绝启动。
    pub fn from_hex_key(
        hex_key: &str,
        max_clock_skew_ms: u64,
    ) -> Result<Self, SagaHttpMessageAuthError> {
        if max_clock_skew_ms == 0 {
            return Err(SagaHttpMessageAuthError::configuration());
        }
        let key = decode_fixed_hex(hex_key).ok_or_else(SagaHttpMessageAuthError::configuration)?;
        let template = HmacSha256::new_from_slice(&key)
            .map_err(|_| SagaHttpMessageAuthError::configuration())?;
        Ok(Self {
            template,
            max_clock_skew_ms,
        })
    }

    /// 业务作用：为一条已持久化 Saga transport payload 生成防篡改身份收据。
    ///
    /// 参数说明：
    /// - `producer`: 此 key 在部署信任表中绑定的逻辑 producer。
    /// - `path`: 接收端固定 Saga path，防止签名跨端点复用。
    /// - `timestamp_ms`: 发送时 Unix 毫秒。
    /// - `nonce`: 每次投递新生成的 128 位小写十六进制随机数。
    /// - `body`: 将被原样发送的 HTTP body。
    ///
    /// 返回：64 字符小写十六进制 HMAC-SHA-256；调用方必须与上述字段一同发送。
    pub fn sign(
        &self,
        producer: &ServiceIdentity,
        path: &str,
        timestamp_ms: u64,
        nonce: &str,
        body: &[u8],
    ) -> String {
        let mut mac = self.template.clone();
        update_mac(&mut mac, producer, path, timestamp_ms, nonce, body);
        encode_hex(&mac.finalize().into_bytes())
    }

    /// 业务作用：在解析 envelope 和占用 Inbox 身份前校验 transport producer 收据。
    ///
    /// 参数说明：
    /// - `message`: 冻结 producer/path/timestamp/nonce/body/signature 的原始收据。
    /// - `now_ms`: 接收端当前 Unix 毫秒。
    ///
    /// 返回：身份、路径、时间窗和 body 全部匹配时成功；任一不符返回同一脱敏错误。
    fn verify(
        &self,
        message: &SagaHttpSignedMessage<'_>,
        now_ms: u64,
    ) -> Result<(), SagaHttpMessageAuthError> {
        if message.timestamp_ms.abs_diff(now_ms) > self.max_clock_skew_ms
            || !valid_nonce(message.nonce)
        {
            return Err(SagaHttpMessageAuthError::authentication());
        }
        let expected = decode_fixed_hex(message.signature)
            .ok_or_else(SagaHttpMessageAuthError::authentication)?;
        let mut mac = self.template.clone();
        update_mac(
            &mut mac,
            message.producer,
            message.path,
            message.timestamp_ms,
            message.nonce,
            message.body,
        );
        mac.verify_slice(&expected)
            .map_err(|_| SagaHttpMessageAuthError::authentication())
    }

    /// 业务作用：生成一条 transport 投递专用的不可预测 nonce，供 replay guard 建立一次性门禁。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：128 位随机 UUID 的 32 字符小写十六进制表示；调用方不得跨请求复用。
    pub fn issue_nonce() -> String {
        Uuid::new_v4().simple().to_string()
    }
}

/// 业务作用：在 HMAC 时间窗内原子拒绝已经验真的 nonce，防止认证请求被原样重放。
///
/// 本结构有意使用有界内存：过期项在每次 claim 前清理，未过期项达到上限时 fail-closed，绝不
/// 淘汰仍可重放的证据。它保护单进程 connector；多副本生产入口必须把 nonce claim 放入共享的
/// 强一致存储，或由网关提供等价的一次性请求语义。
pub struct SagaHttpReplayGuard {
    max_entries: usize,
    claims: Mutex<ReplayClaims>,
    accepted_total: AtomicU64,
    authentication_failed_total: AtomicU64,
    replay_rejected_total: AtomicU64,
    capacity_rejected_total: AtomicU64,
    lock_poison_recovered_total: AtomicU64,
}

/// 业务作用：维护 nonce 主索引与过期时间索引，使请求清理成本只与本次实际过期项数量相关。
#[derive(Default)]
struct ReplayClaims {
    by_nonce: BTreeMap<(ServiceIdentity, String), u64>,
    by_expiry: BTreeMap<u64, BTreeSet<(ServiceIdentity, String)>>,
}

impl ReplayClaims {
    /// 业务作用：清理已越过 replay horizon 的 claim，而不在每次请求扫描全部活跃 nonce。
    ///
    /// 参数说明：
    /// - `now_ms`: 当前接收时钟；仅清除过期点严格小于该值的记录。
    ///
    /// 返回：无；两个索引保持同步，仍在时间窗内的 claim 不被淘汰。
    fn remove_expired(&mut self, now_ms: u64) {
        while let Some(expires_at_ms) = self
            .by_expiry
            .first_key_value()
            .map(|(&expires_at_ms, _)| expires_at_ms)
        {
            // 验签时间窗包含 `abs_diff == skew` 的边界，因此 claim 在 expires_at 当毫秒仍必须保留。
            if expires_at_ms >= now_ms {
                break;
            }
            let Some(keys) = self.by_expiry.remove(&expires_at_ms) else {
                break;
            };
            for key in keys {
                if self.by_nonce.get(&key) == Some(&expires_at_ms) {
                    self.by_nonce.remove(&key);
                }
            }
        }
    }

    /// 业务作用：把首次出现的 nonce 同时写入主索引和过期索引，保证后续可对数定位与清理。
    ///
    /// 参数说明：
    /// - `key`: producer 与 nonce 组成的信任边内唯一身份。
    /// - `expires_at_ms`: 最晚仍需拒绝原报文的时间。
    ///
    /// 返回：无；调用方已在同一锁内确认 key 不存在且容量可用。
    fn insert(&mut self, key: (ServiceIdentity, String), expires_at_ms: u64) {
        self.by_nonce.insert(key.clone(), expires_at_ms);
        self.by_expiry.entry(expires_at_ms).or_default().insert(key);
    }
}

/// 业务作用：导出单条 HTTP 信任边的低基数 replay 防护快照，区分攻击、重放与容量配置问题。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SagaHttpReplayMetrics {
    /// 当前仍在时间窗内的 nonce 数。
    pub active_claims: u64,
    /// 本 guard 的硬容量；达到后 fail-closed。
    pub capacity: u64,
    /// 历史首次验真并成功 claim 的请求数。
    pub accepted_total: u64,
    /// 历史 HMAC、时间窗或 nonce 编码认证失败数。
    pub authentication_failed_total: u64,
    /// 历史 exact replay 拒绝数。
    pub replay_rejected_total: u64,
    /// 历史容量耗尽拒绝数。
    pub capacity_rejected_total: u64,
    /// 历史 mutex 中毒后恢复并继续提供服务的次数。
    pub lock_poison_recovered_total: u64,
}

impl SagaHttpReplayMetrics {
    /// 业务作用：计算不使用浮点数的容量利用率，供固定阈值告警比较。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：0..=1000 的千分比；非法零容量按 1000 返回以保持 fail-closed 可见性。
    pub fn utilization_per_mille(self) -> u64 {
        if self.capacity == 0 {
            return 1_000;
        }
        self.active_claims
            .saturating_mul(1_000)
            .checked_div(self.capacity)
            .unwrap_or(1_000)
            .min(1_000)
    }
}

/// 业务作用：限定 replay 指标允许使用的低基数平面名，禁止把 producer/path 等用户维度写入时序。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SagaHttpReplayPlane {
    /// Participant command 接收面。
    Command,
    /// Orchestrator result 接收面。
    Result,
    /// Orchestrator 管理与恢复面。
    Management,
}

impl SagaHttpReplayPlane {
    /// 业务作用：返回 Prometheus 指标使用的固定平面片段。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：只可能是 `command`、`result` 或 `management`，不包含外部输入。
    fn as_str(self) -> &'static str {
        match self {
            Self::Command => "command",
            Self::Result => "result",
            Self::Management => "management",
        }
    }
}

/// 业务作用：聚合同一 transport 平面内互相隔离的 replay guards，同时保留最拥挤单边水位。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SagaHttpReplayMetricAggregate {
    guard_edges: u64,
    active_claims: u64,
    capacity: u64,
    max_utilization_per_mille: u64,
    accepted_total: u64,
    authentication_failed_total: u64,
    replay_rejected_total: u64,
    capacity_rejected_total: u64,
    lock_poison_recovered_total: u64,
}

impl SagaHttpReplayMetricAggregate {
    /// 业务作用：合并一条独立信任边的快照，且不引入 producer/path 高基数 label。
    ///
    /// 参数说明：
    /// - `metrics`: 单个 replay guard 在同一采集时钟下生成的快照。
    ///
    /// 返回：无；总量使用饱和加法，水位保留单边最大值而不是会掩盖热点的整体平均值。
    pub fn include(&mut self, metrics: SagaHttpReplayMetrics) {
        self.guard_edges = self.guard_edges.saturating_add(1);
        self.active_claims = self.active_claims.saturating_add(metrics.active_claims);
        self.capacity = self.capacity.saturating_add(metrics.capacity);
        self.max_utilization_per_mille = self
            .max_utilization_per_mille
            .max(metrics.utilization_per_mille());
        self.accepted_total = self.accepted_total.saturating_add(metrics.accepted_total);
        self.authentication_failed_total = self
            .authentication_failed_total
            .saturating_add(metrics.authentication_failed_total);
        self.replay_rejected_total = self
            .replay_rejected_total
            .saturating_add(metrics.replay_rejected_total);
        self.capacity_rejected_total = self
            .capacity_rejected_total
            .saturating_add(metrics.capacity_rejected_total);
        self.lock_poison_recovered_total = self
            .lock_poison_recovered_total
            .saturating_add(metrics.lock_poison_recovered_total);
    }

    /// 业务作用：把聚合快照渲染为稳定、无 label 的 Prometheus 文本，供告警规则直接引用。
    ///
    /// 参数说明：
    /// - `plane`: 编译期封闭的 command/result/management 平面名。
    ///
    /// 返回：包含 guard 数、容量水位及认证/重放/容量/锁恢复分类计数的文本。
    pub fn render_prometheus(self, plane: SagaHttpReplayPlane) -> String {
        let plane = plane.as_str();
        format!(
            concat!(
                "# TYPE nasaga_http_{0}_replay_guard_edges gauge\n",
                "nasaga_http_{0}_replay_guard_edges {1}\n",
                "# TYPE nasaga_http_{0}_replay_active gauge\n",
                "nasaga_http_{0}_replay_active {2}\n",
                "# TYPE nasaga_http_{0}_replay_capacity gauge\n",
                "nasaga_http_{0}_replay_capacity {3}\n",
                "# TYPE nasaga_http_{0}_replay_max_utilization_per_mille gauge\n",
                "nasaga_http_{0}_replay_max_utilization_per_mille {4}\n",
                "# TYPE nasaga_http_{0}_replay_accepted_total counter\n",
                "nasaga_http_{0}_replay_accepted_total {5}\n",
                "# TYPE nasaga_http_{0}_replay_authentication_failed_total counter\n",
                "nasaga_http_{0}_replay_authentication_failed_total {6}\n",
                "# TYPE nasaga_http_{0}_replay_rejected_total counter\n",
                "nasaga_http_{0}_replay_rejected_total {7}\n",
                "# TYPE nasaga_http_{0}_replay_capacity_rejected_total counter\n",
                "nasaga_http_{0}_replay_capacity_rejected_total {8}\n",
                "# TYPE nasaga_http_{0}_replay_lock_poison_recovered_total counter\n",
                "nasaga_http_{0}_replay_lock_poison_recovered_total {9}\n"
            ),
            plane,
            self.guard_edges,
            self.active_claims,
            self.capacity,
            self.max_utilization_per_mille,
            self.accepted_total,
            self.authentication_failed_total,
            self.replay_rejected_total,
            self.capacity_rejected_total,
            self.lock_poison_recovered_total,
        )
    }
}

/// 业务作用：渲染 HTTP command 确定性协议拒绝已持久化到 DLT 的低基数总量。
///
/// 参数说明：
/// - `total`: 与 DLT 行同事务、按 event_id 只累计一次的 durable 历史总量。
///
/// 返回：固定指标名、无 label 的 Prometheus counter 文本，供轻量 connector 与告警合同共用。
pub fn render_saga_http_command_dlt_metric(total: u64) -> String {
    format!("# TYPE nasaga_http_command_dlt_total counter\nnasaga_http_command_dlt_total {total}\n")
}

impl SagaHttpReplayGuard {
    /// 业务作用：构造有界 replay guard，限制攻击流量可占用的 nonce 记录数。
    ///
    /// 参数说明：
    /// - `max_entries`: 同一进程允许保留的未过期 nonce 上限，必须大于零。
    ///
    /// 返回：上限合法时返回空 guard；零容量直接拒绝启动。
    pub fn new(max_entries: usize) -> Result<Self, SagaHttpMessageAuthError> {
        if max_entries == 0 {
            return Err(SagaHttpMessageAuthError::configuration());
        }
        Ok(Self {
            max_entries,
            claims: Mutex::new(ReplayClaims::default()),
            accepted_total: AtomicU64::new(0),
            authentication_failed_total: AtomicU64::new(0),
            replay_rejected_total: AtomicU64::new(0),
            capacity_rejected_total: AtomicU64::new(0),
            lock_poison_recovered_total: AtomicU64::new(0),
        })
    }

    /// 业务作用：先校验完整 HMAC，再原子占用 producer/nonce，形成仅可消费一次的身份凭据。
    ///
    /// 参数说明：
    /// - `authenticator`: 当前部署信任边绑定的 HMAC 认证器。
    /// - `message`: 冻结 producer/path/timestamp/nonce/body/signature 的原始收据。
    /// - `now_ms`: 接收端当前 Unix 毫秒。
    ///
    /// 返回：签名有效且 nonce 首次出现时成功；重放、容量耗尽或认证失败均脱敏拒绝。
    pub fn verify_once(
        &self,
        authenticator: &SagaHttpMessageAuthenticator,
        message: &SagaHttpSignedMessage<'_>,
        now_ms: u64,
    ) -> Result<(), SagaHttpMessageAuthError> {
        if let Err(error) = authenticator.verify(message, now_ms) {
            saturating_increment(&self.authentication_failed_total);
            return Err(error);
        }

        let mut claims = self.lock_claims();
        claims.remove_expired(now_ms);
        let key = (message.producer.clone(), message.nonce.to_owned());
        if claims.by_nonce.contains_key(&key) {
            saturating_increment(&self.replay_rejected_total);
            return Err(SagaHttpMessageAuthError::replay());
        }
        if claims.by_nonce.len() >= self.max_entries {
            saturating_increment(&self.capacity_rejected_total);
            return Err(SagaHttpMessageAuthError::capacity());
        }
        // claim 必须保留到该签名时间窗的闭区间右端；若在等于右端时提前清理，原报文仍能通过
        // `abs_diff <= skew` 并获得第二次执行机会。未来偏移到窗口上沿的签名最多占用约 2×skew。
        let expires_at_ms = message
            .timestamp_ms
            .saturating_add(authenticator.max_clock_skew_ms);
        claims.insert(key, expires_at_ms);
        saturating_increment(&self.accepted_total);
        Ok(())
    }

    /// 业务作用：读取并顺带清理一条信任边的 replay 防护指标，供控制面区分攻击与容量不足。
    ///
    /// 参数说明：
    /// - `now_ms`: 指标采集时钟，用于剔除已经越过 replay horizon 的 active claim。
    ///
    /// 返回：不含 producer、nonce、path 或 payload 的低基数计数快照。
    pub fn metrics(&self, now_ms: u64) -> SagaHttpReplayMetrics {
        let mut claims = self.lock_claims();
        claims.remove_expired(now_ms);
        SagaHttpReplayMetrics {
            active_claims: u64::try_from(claims.by_nonce.len()).unwrap_or(u64::MAX),
            capacity: u64::try_from(self.max_entries).unwrap_or(u64::MAX),
            accepted_total: self.accepted_total.load(Ordering::Relaxed),
            authentication_failed_total: self.authentication_failed_total.load(Ordering::Relaxed),
            replay_rejected_total: self.replay_rejected_total.load(Ordering::Relaxed),
            capacity_rejected_total: self.capacity_rejected_total.load(Ordering::Relaxed),
            lock_poison_recovered_total: self.lock_poison_recovered_total.load(Ordering::Relaxed),
        }
    }

    /// 业务作用：取得 claims 锁并在中毒时恢复受保护索引，避免一次 panic 永久关闭认证面。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可继续完成原子 claim/清理的锁 guard；发生过中毒时同步增加可观测计数。
    fn lock_claims(&self) -> MutexGuard<'_, ReplayClaims> {
        match self.claims.lock() {
            Ok(claims) => claims,
            Err(poisoned) => {
                saturating_increment(&self.lock_poison_recovered_total);
                // 本临界区只执行不可失败的内存索引更新；恢复 guard 后清除 poison，避免同一故障
                // 被后续每个认证请求重复计数并持续污染运维判断。
                self.claims.clear_poison();
                poisoned.into_inner()
            }
        }
    }
}

/// 业务作用：表示 Saga HTTP 收据拒绝的内部稳定分类，外部响应仍必须统一脱敏。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SagaHttpMessageAuthFailure {
    /// key、容量或时钟窗口配置非法，listener 不得 Ready。
    ConfigurationInvalid,
    /// HMAC、时间窗或 nonce 编码不匹配。
    AuthenticationFailed,
    /// producer/nonce 已在当前 replay horizon 内消费。
    ReplayDetected,
    /// 当前信任边的未过期 nonce 已达到硬容量。
    CapacityExhausted,
}

/// 业务作用：携带可观测但不含凭据的内部认证失败分类，同时保持统一对外错误文本。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SagaHttpMessageAuthError {
    failure: SagaHttpMessageAuthFailure,
}

impl SagaHttpMessageAuthError {
    /// 业务作用：读取安全的内部失败分类，驱动日志、指标和容量告警，不用于改变外部错误正文。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：配置、认证、重放或容量耗尽中的稳定分类。
    pub fn failure(self) -> SagaHttpMessageAuthFailure {
        self.failure
    }

    /// 业务作用：构造启动配置拒绝。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不包含敏感配置值的 `ConfigurationInvalid` 错误。
    fn configuration() -> Self {
        Self {
            failure: SagaHttpMessageAuthFailure::ConfigurationInvalid,
        }
    }

    /// 业务作用：构造密码学或时钟认证拒绝。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不暴露具体失败字段的 `AuthenticationFailed` 错误。
    fn authentication() -> Self {
        Self {
            failure: SagaHttpMessageAuthFailure::AuthenticationFailed,
        }
    }

    /// 业务作用：构造 exact replay 拒绝。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可供内部指标分类的 `ReplayDetected` 错误。
    fn replay() -> Self {
        Self {
            failure: SagaHttpMessageAuthFailure::ReplayDetected,
        }
    }

    /// 业务作用：构造单信任边容量耗尽拒绝。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：可供内部容量告警分类的 `CapacityExhausted` 错误。
    fn capacity() -> Self {
        Self {
            failure: SagaHttpMessageAuthFailure::CapacityExhausted,
        }
    }
}

impl std::fmt::Display for SagaHttpMessageAuthError {
    /// 业务作用：输出稳定的认证拒绝文案。
    ///
    /// 参数说明：
    /// - `formatter`: 标准格式化目标。
    ///
    /// 返回：写入成功返回 `Ok`，格式化失败返回底层错误。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("saga transport message authentication failed")
    }
}

impl std::error::Error for SagaHttpMessageAuthError {}

/// 业务作用：以饱和语义增加认证计数，避免极端长生命周期进程发生 u64 回绕。
///
/// 参数说明：
/// - `counter`: 待增加的低基数原子计数器。
///
/// 返回：无；并发竞争时由原子更新重试，不获取 claims 锁。
fn saturating_increment(counter: &AtomicU64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(1))
    });
}

/// 业务作用：按固定域分隔顺序把身份、路径、时间、nonce 和 body 纳入 HMAC，防止字段拼接歧义。
///
/// 参数说明：
/// - `mac`: 已由当前信任边 key 初始化的 HMAC。
/// - `producer`: key 绑定的逻辑 producer。
/// - `path`: 固定接收路径。
/// - `timestamp_ms`: 发送时间。
/// - `nonce`: 本次投递的一次性随机数。
/// - `body`: 原始消息字节。
///
/// 返回：无；只更新本地 HMAC 状态，不记录或外发敏感内容。
fn update_mac(
    mac: &mut HmacSha256,
    producer: &ServiceIdentity,
    path: &str,
    timestamp_ms: u64,
    nonce: &str,
    body: &[u8],
) {
    mac.update(b"nasaga-http-v2\n");
    mac.update(producer.as_str().as_bytes());
    mac.update(b"\n");
    mac.update(path.as_bytes());
    mac.update(b"\n");
    mac.update(timestamp_ms.to_string().as_bytes());
    mac.update(b"\n");
    mac.update(nonce.as_bytes());
    mac.update(b"\n");
    mac.update(body);
}

/// 业务作用：严格校验 nonce 的固定长度与小写十六进制编码，消除多种文本表示带来的重放歧义。
///
/// 参数说明：
/// - `nonce`: 预期为 32 字符小写十六进制文本。
///
/// 返回：编码唯一且恰好表示 128 位随机数时为 `true`。
fn valid_nonce(nonce: &str) -> bool {
    nonce.len() == 32
        && nonce
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

/// 业务作用：严格解析 256 位小写十六进制 key 或签名，拒绝宽松大小写和截断输入。
///
/// 参数说明：
/// - `value`: 预期为 64 字符小写十六进制文本。
///
/// 返回：合法时返回 32 字节；否则返回 `None`。
fn decode_fixed_hex(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64 {
        return None;
    }
    let mut decoded = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = decode_nibble(pair[0])?;
        let low = decode_nibble(pair[1])?;
        decoded[index] = (high << 4) | low;
    }
    Some(decoded)
}

/// 业务作用：解析单个小写十六进制半字节，不接受大写或其它可混淆字符。
///
/// 参数说明：
/// - `value`: ASCII 字节。
///
/// 返回：合法半字节返回 0..15；否则返回 `None`。
fn decode_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

/// 业务作用：把 HMAC 字节编码为稳定小写十六进制 header 值。
///
/// 参数说明：
/// - `bytes`: HMAC-SHA-256 输出。
///
/// 返回：不含前缀的 64 字符小写十六进制文本。
fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}
