// ============================================================================
// src/config.rs —— 配置类(文档;默认值对齐 配置全景表,即 原实现 源码默认)。
//
// 命名约定:
//   · legacy = 历史协议/历史运行模式。它影响 wire 名、锁语义、marker、路由等持久化边界。
//   · compat = 单个兼容算法/格式化函数。它只复刻某个局部规则,不代表整套运行模式。
//
// 纪律
//   · CompatibilityProfile 没有 Default,RedisConfig 反序列化缺 profile 即失败——
//     这是持久化/跨语言协议,不允许新项目无意承担 原实现V1 约束,也不允许误连;
//   · namespace 必填:协议标记 nasa:protocol:{namespace} 的作用域。
// 装配模型:业务在 #[tokio::main] 里构造本配置 → RedisClient::connect(cfg)。
// ============================================================================

use serde::{Deserialize, Serialize};

/// 托管 Stream 单次读取的硬上限。
///
/// `COUNT` 只是 Redis 的返回数量提示，不会替客户端限制响应体字节；运行时仍需限制单轮 entry 数，
/// 避免误配置把一次读取、解析和 handler 分发同时放大。高吞吐应靠持续批量和受控在飞并发，而不是
/// 让单次响应无限增长。
pub(crate) const MAX_STREAM_BATCH_SIZE: usize = 10_000;
/// 单个 Redis 协议名称或 partition topic 的 UTF-8 字节上限。
pub(crate) const MAX_REDIS_NAME_BYTES: usize = 256;
/// 单个运行时允许的隔离组数量；每组都会创建独立 coordinator 和维护任务。
pub(crate) const MAX_PARTITION_GROUPS: usize = 256;
/// 默认组与全部隔离组的 resolved 分区数总和上限。
pub(crate) const MAX_TOTAL_PARTITIONS: u64 = 65_536;
/// 外部配置可声明的 topic 总数上限。
pub(crate) const MAX_PARTITION_TOPICS: usize = 4_096;
/// Tokio 定时器驱动的 Redis 运行参数统一使用工作区的一年上限。
pub(crate) const MAX_REDIS_RUNTIME_DURATION_MS: u64 = 365 * 24 * 60 * 60 * 1_000;

/// 兼容性 profile:决定 key 布局/心跳时钟/锁协议/ACK 协议/Stream 事件编码,
/// 业务只能整体选择,不能拼出半兼容组合。**无 Default,必须显式指定。**
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompatibilityProfile {
    /// 原实现 互通/灰度:key 命名、锁 Lua、wire 与 原实现 逐字节一致。
    /// legacy 表示整套历史协议模式,不是单个 helper 的局部兼容逻辑。
    LegacyV1,
    /// 纯 Rust 集群：启用 V2 fencing 任期 stamp 与原子 fenced ACK 等增强能力。
    /// **心跳时钟**:
    ///   · **RustV2 = Redis TIME**(`nodes` ZSET 分数与过期驱逐 `now_ms` 同走服务端时钟,全集群单一
    ///     时钟源,节点墙钟漂移不再误判存活);
    ///   · 原实现V1/原实现 = 本地墙钟(与 原实现 节点逐字节互通必须同时基)。
    /// ⚠ **RustV2 节点不得与墙钟(原实现V1/原实现)节点共享同一 group 的 `nodes` ZSET**——两类用不同
    /// 时基写分数 + 各用自己的 now_ms 驱逐,时钟差几秒即互相误判过期 ZREM → 虚假 owner 抖动/双 claim
    /// 窗口。(1 把本注释误改为"两 profile 同墙钟"与代码相反,本轮据实复原。)
    RustV2,
}

impl CompatibilityProfile {
    /// 业务作用：**持久化标识名**(protocol marker / naming_config canonical 用),与 Rust 枚举名解耦。
    /// `LegacyV1` 的 wire 名固定为历史值 **`"LegacyV1"`**:marker 是落盘的持久 wire,改 Rust 枚举名
    /// (去品牌化)**不能**改已写入的 marker 标识,否则既有 namespace 升级后 profile 名不符会
    /// fail-closed 拒启动。marker 比对/生成一律用本方法而非 `format!("{self:?}")`。(RustV2 wire 名不变。)
    pub fn wire_name(&self) -> &'static str {
        match self {
            CompatibilityProfile::LegacyV1 => "LegacyV1",
            CompatibilityProfile::RustV2 => "RustV2",
        }
    }
}

/// 顶层配置。`profile` 与 `namespace` 必填,其余有生产可用默认值(对齐)。
///
/// `Debug` 手写脱敏:`url` 内可能内嵌密码(`redis://:pass@host`),打印时去掉 userinfo,
/// 避免下游 `tracing::debug!(?cfg)` / `{:?}` 把连接串密码泄漏进日志(公共库默认防御)。
#[derive(Clone, Deserialize)]
pub struct RedisConfig {
    /// Redis 连接串,格式如 `redis://[:password@]host:port[/db]`。
    pub url: String,
    /// 协议命名空间(协议标记 key 的作用域;通常 = 业务系统名)。
    pub namespace: String,
    /// 兼容性 profile,无默认值。
    pub profile: CompatibilityProfile,
    #[serde(default)]
    /// 单条命令执行超时与慢日志配置。
    pub command: CommandCfg,
    #[serde(default)]
    /// 显式/自动 pipeline 的批量和等待配置。
    pub pipeline: PipelineCfg,
    #[serde(default)]
    /// 分布式锁 TTL、等待和看门狗续期配置。
    pub lock: LockCfg,
    #[serde(default)]
    /// Redis Stream 订阅与消费配置。
    pub stream: StreamCfg,
    #[serde(default)]
    /// 分区消费配置。
    pub partition: PartitionCfg,
}

/// 业务作用：去掉 redis 连接串里的 userinfo(`scheme://[user][:password]@` → `scheme://***@`),用于脱敏日志。
/// 无 userinfo(如 `redis://host:port`)则原样返回。
pub(crate) fn redact_url(url: &str) -> String {
    if let Some(scheme_end) = url.find("://") {
        let after = scheme_end + 3;
        if let Some(at_rel) = url[after..].find('@') {
            let at = after + at_rel;
            return format!("{}***{}", &url[..after], &url[at..]);
        }
    }
    url.to_string()
}

impl std::fmt::Debug for RedisConfig {
    /// 业务作用：输出 Redis 配置的调试视图;url 会移除 userinfo,避免日志泄露凭据。
    ///
    /// # 参数
    /// - `f`: Debug 或 Display 输出使用的标准格式化器。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisConfig")
            .field("url", &redact_url(&self.url))
            .field("namespace", &self.namespace)
            .field("profile", &self.profile)
            .field("command", &self.command)
            .field("pipeline", &self.pipeline)
            .field("lock", &self.lock)
            .field("stream", &self.stream)
            .field("partition", &self.partition)
            .finish()
    }
}

impl RedisConfig {
    /// 业务作用：代码内构造(替代配置文件;profile 仍强制显式)。
    ///
    /// # 参数
    /// - `url`: Redis 连接串。
    /// - `namespace`: 协议命名空间,用于协议标记和 key 前缀隔离。
    /// - `profile`: 兼容性 profile,决定持久化 key 布局和运行协议。
    pub fn new(
        url: impl Into<String>,
        namespace: impl Into<String>,
        profile: CompatibilityProfile,
    ) -> Self {
        Self {
            url: url.into(),
            namespace: namespace.into(),
            profile,
            command: CommandCfg::default(),
            pipeline: PipelineCfg::default(),
            lock: LockCfg::default(),
            stream: StreamCfg::default(),
            partition: PartitionCfg::default(),
        }
    }

    /// 业务作用：返回可安全写入日志与错误的定位信息:`scheme://host:port[/db]`,移除 userinfo(用户名/密码)与
    /// query/fragment(可能含 TLS 私钥路径等)。
    ///
    /// 供启动连接诊断复用:错误摘要凭它定位是哪个 host/port/db 失败,而不泄漏连接串凭据。无法解析(无
    /// `://` 或 host 段为空)时返回固定占位符 `<unknown-endpoint>`,**绝不**回退打印含凭据的原始连接串。
    ///
    /// # 参数
    ///
    /// 本方法无参数;只读取自身 `url`,不访问网络。
    pub fn safe_endpoint(&self) -> String {
        let url = self.url.trim();
        let Some(scheme_end) = url.find("://") else {
            return "<unknown-endpoint>".to_owned();
        };
        let scheme = &url[..scheme_end];
        let after = &url[scheme_end + 3..];
        // 去掉 userinfo(`@` 之前的 user[:password]);IPv6 host 形如 `[::1]:7001` 不含 `@`,不受影响。
        let host_and_rest = match after.rfind('@') {
            Some(at) => &after[at + 1..],
            None => after,
        };
        // 去掉 query/fragment,只留 host:port[/db]。
        let host_and_path = host_and_rest
            .split(['?', '#'])
            .next()
            .unwrap_or(host_and_rest);
        if host_and_path.is_empty() {
            return "<unknown-endpoint>".to_owned();
        }
        format!("{scheme}://{host_and_path}")
    }

    /// 业务作用：启动期校验(connect 内调用)。
    ///
    pub fn validate(&self) -> crate::error::Result<()> {
        let semaphore_max = tokio::sync::Semaphore::MAX_PERMITS;
        if self.url.is_empty() {
            return Err(crate::error::NasaRedisError::Config("url 为空".into()));
        }
        if self.namespace.trim().is_empty()
            || self.namespace != self.namespace.trim()
            || self.namespace.len() > MAX_REDIS_NAME_BYTES
        {
            return Err(crate::error::NasaRedisError::Config(format!(
                "namespace 必须为无首尾空白的非空名称，且不超过 {MAX_REDIS_NAME_BYTES} 字节"
            )));
        }
        if self.lock.lease_ms < 3_000 {
            return Err(crate::error::NasaRedisError::Config(
                "lock.lease_ms 过小(<3s):看门狗周期 lease/3 无意义".into(),
            ));
        }
        //lease 上界——过大 → 看门狗 interval=lease/3 巨大,首次续期前崩溃锁残留超长
        // (新 owner 要等到 lease 过期才能接管)。上界 5min(300s)对绝大多数场景足够宽松。
        if self.lock.lease_ms > 300_000 {
            return Err(crate::error::NasaRedisError::Config(format!(
                "lock.lease_ms 过大({}ms > 300s):崩溃后锁残留至 lease 过期、阻塞接管过久",
                self.lock.lease_ms
            )));
        }
        // ── 分区/流参数 fail-fast(count=0 会让 route_str
        //    除零 panic;rebalance_ms=0 会让 tokio::time::interval(ZERO) panic;
        //    其余 0 值会让 batch/budget 退化。配置存在却不生效或直接 panic,比不
        //    提供配置更危险)──
        let cfg = |msg: String| Err(crate::error::NasaRedisError::Config(msg));
        if self.command.timeout_ms > MAX_REDIS_RUNTIME_DURATION_MS {
            return cfg(format!(
                "command.timeout_ms 过大(上限 {MAX_REDIS_RUNTIME_DURATION_MS}ms；0 表示不限)"
            ));
        }
        if self.command.response_timeout_ms > MAX_REDIS_RUNTIME_DURATION_MS {
            return cfg(format!(
                "command.response_timeout_ms 过大(上限 {MAX_REDIS_RUNTIME_DURATION_MS}ms；0 表示不限)"
            ));
        }
        if self.partition.count == 0 {
            return cfg("partition.count 必须 > 0(否则路由除零 panic)".into());
        }
        // 单组容量上界：event channel、claimed Vec 和每轮再平衡扫描均随 count 线性增长。
        // 16_384 只是运行时资源保护值，不代表 Cluster 能把这些分区分散到 16_384 个 slot；
        // Cluster 为满足多 key Lua 同槽，会把整个组固定到一个 slot。需要跨 master 扩展时拆隔离组。
        const MAX_PARTITION_COUNT: u32 = 16_384;
        if self.partition.count > MAX_PARTITION_COUNT {
            return cfg(format!(
                "partition.count 过大({},上限 {MAX_PARTITION_COUNT}):单组运行时资源随分区数线性增长；\
                 更大规模请拆分多个隔离组(Cluster 下每组固定到一个 slot)",
                self.partition.count,
            ));
        }
        if self.partition.default_group.trim().is_empty()
            || self.partition.default_group != self.partition.default_group.trim()
            || self.partition.default_group.len() > MAX_REDIS_NAME_BYTES
            || self.partition.default_group.contains(['{', '}', ':'])
        {
            return cfg(format!(
                "partition.default_group 必须为无首尾空白的非空名称，不得含 `{{`/`}}`/`:`，且不超过 {MAX_REDIS_NAME_BYTES} 字节"
            ));
        }
        if self.partition.groups.len() > MAX_PARTITION_GROUPS {
            return cfg(format!(
                "partition.groups 数量过大({},上限 {MAX_PARTITION_GROUPS})",
                self.partition.groups.len()
            ));
        }
        // 隔离组校验:逻辑名 trim 非空 + 不含 `{`/`}`;topics trim 非空;count 套上界;
        // **跨组 topic 唯一**(connect 期 fail-fast,prepare 再设二道防线)。
        {
            let mut seen_topics: std::collections::HashSet<&str> = std::collections::HashSet::new();
            let mut total_partitions = u64::from(self.partition.count);
            let mut total_topics = 0_usize;
            for (logical, g) in &self.partition.groups {
                if logical.trim().is_empty()
                    || logical != logical.trim()
                    || logical.len() > MAX_REDIS_NAME_BYTES
                    || logical.contains(['{', '}', ':'])
                {
                    return cfg(format!(
                        "partition.groups 含非法逻辑名:不得为空/含首尾空白/`{{`/`}}`/`:`,且不得超过 {MAX_REDIS_NAME_BYTES} 字节"
                    ));
                }
                let gcount = if g.count > 0 {
                    g.count
                } else {
                    self.partition.count
                };
                if gcount > MAX_PARTITION_COUNT {
                    return cfg(format!(
                        "partition.groups[\"{logical}\"].count 过大({gcount},上限 {MAX_PARTITION_COUNT})"
                    ));
                }
                total_partitions =
                    total_partitions
                        .checked_add(u64::from(gcount))
                        .ok_or_else(|| {
                            crate::error::NasaRedisError::Config(
                                "partition resolved 分区总数溢出".into(),
                            )
                        })?;
                if total_partitions > MAX_TOTAL_PARTITIONS {
                    return cfg(format!(
                        "partition resolved 分区总数过大({total_partitions},上限 {MAX_TOTAL_PARTITIONS})"
                    ));
                }
                // per-group 运行时覆盖范围校验(Some 时套与父级同样约束:0 fail-fast、rebalance ≤1h)
                let bad0 =
                    |name: &str| cfg(format!("partition.groups[\"{logical}\"].{name} 必须 > 0"));
                if g.rebalance_ms == Some(0) {
                    return bad0("rebalance_ms");
                }
                if let Some(v) = g.rebalance_ms {
                    if v > 3_600_000 {
                        return cfg(format!(
                            "partition.groups[\"{logical}\"].rebalance_ms 过大({v}ms,上限 1h)"
                        ));
                    }
                }
                if g.min_idle_ms == Some(0) {
                    return bad0("min_idle_ms");
                }
                if g.min_idle_ms
                    .is_some_and(|value| value > MAX_REDIS_RUNTIME_DURATION_MS)
                {
                    return cfg(format!(
                        "partition.groups[\"{logical}\"].min_idle_ms 过大(上限 {MAX_REDIS_RUNTIME_DURATION_MS}ms)"
                    ));
                }
                if g.holds_check_interval_ms == Some(0) {
                    return bad0("holds_check_interval_ms");
                }
                if g.holds_check_interval_ms
                    .is_some_and(|value| value > MAX_REDIS_RUNTIME_DURATION_MS)
                {
                    return cfg(format!(
                        "partition.groups[\"{logical}\"].holds_check_interval_ms 过大(上限 {MAX_REDIS_RUNTIME_DURATION_MS}ms)"
                    ));
                }
                if g.drain_timeout_ms == Some(0) {
                    return bad0("drain_timeout_ms");
                }
                if g.drain_timeout_ms
                    .is_some_and(|value| value > MAX_REDIS_RUNTIME_DURATION_MS)
                {
                    return cfg(format!(
                        "partition.groups[\"{logical}\"].drain_timeout_ms 过大(上限 {MAX_REDIS_RUNTIME_DURATION_MS}ms)"
                    ));
                }
                if g.handler_timeout_ms == Some(0) {
                    return bad0("handler_timeout_ms");
                }
                if g.handler_timeout_ms
                    .is_some_and(|value| value > MAX_REDIS_RUNTIME_DURATION_MS)
                {
                    return cfg(format!(
                        "partition.groups[\"{logical}\"].handler_timeout_ms 过大(上限 {MAX_REDIS_RUNTIME_DURATION_MS}ms)"
                    ));
                }
                if g.batch_size == Some(0) {
                    return bad0("batch_size");
                }
                if g.batch_size
                    .is_some_and(|value| value > MAX_STREAM_BATCH_SIZE)
                {
                    return cfg(format!(
                        "partition.groups[\"{logical}\"].batch_size 过大(上限 {MAX_STREAM_BATCH_SIZE})"
                    ));
                }
                if g.inflight_max == Some(0) {
                    return bad0("inflight_max");
                }
                if g.inflight_max.is_some_and(|value| value > semaphore_max) {
                    return cfg(format!(
                        "partition.groups[\"{logical}\"].inflight_max 超过 Tokio semaphore 容量上限 {semaphore_max}"
                    ));
                }
                if g.poll_timeout_ms == Some(0) {
                    return bad0("poll_timeout_ms");
                }
                if g.poll_timeout_ms
                    .is_some_and(|value| value > MAX_REDIS_RUNTIME_DURATION_MS)
                {
                    return cfg(format!(
                        "partition.groups[\"{logical}\"].poll_timeout_ms 过大(上限 {MAX_REDIS_RUNTIME_DURATION_MS}ms)"
                    ));
                }
                // max_redeliver=0 = 首投即判毒(与 proxy 同纪律 fail-fast)。
                if g.max_redeliver == Some(0) {
                    return cfg(format!(
                        "partition.groups[\"{logical}\"].max_redeliver 必须 >= 1(否则首投即判毒)"
                    ));
                }
                let resolved_drain = g
                    .drain_timeout_ms
                    .unwrap_or(self.partition.drain_timeout_ms);
                let resolved_handler = g
                    .handler_timeout_ms
                    .unwrap_or(self.partition.handler_timeout_ms);
                if resolved_drain < resolved_handler {
                    tracing::warn!(
                        group = %logical,
                        drain_timeout_ms = resolved_drain,
                        handler_timeout_ms = resolved_handler,
                        "隔离组 drain_timeout_ms < handler_timeout_ms：停机时在途 handler 可能在优雅排水完成前被中断"
                    );
                }
                let topics: Vec<&str> = if g.topics.is_empty() {
                    vec![logical.as_str()] // 空 topics → 逻辑名即 topic(对照 原实现)
                } else {
                    g.topics.iter().map(|s| s.as_str()).collect()
                };
                total_topics = total_topics.checked_add(topics.len()).ok_or_else(|| {
                    crate::error::NasaRedisError::Config("partition topic 配置数量溢出".into())
                })?;
                if total_topics > MAX_PARTITION_TOPICS {
                    return cfg(format!(
                        "partition topic 配置总数过大({total_topics},上限 {MAX_PARTITION_TOPICS})"
                    ));
                }
                for t in topics {
                    if t.trim().is_empty() || t != t.trim() || t.len() > MAX_REDIS_NAME_BYTES {
                        return cfg(format!(
                            "partition.groups[\"{logical}\"] 含非法 topic:必须无首尾空白、非空且不超过 {MAX_REDIS_NAME_BYTES} 字节"
                        ));
                    }
                    if !seen_topics.insert(t) {
                        return cfg(format!(
                            "topic \"{t}\" 被多个隔离组路由(跨组 topic 必须唯一,一个 topic 只能属一个组)"
                        ));
                    }
                }
            }
        }
        if self.partition.rebalance_ms == 0 {
            return cfg("partition.rebalance_ms 必须 > 0(interval(0) 会 panic)".into());
        }
        //上界——`rebalance.rs` 心跳过期分数 `now + 3*rebalance_ms` 用 u64 乘法,
        // 极端大值理论可溢出/wrap。1h 远超任何合理再平衡周期,超出即配置错误 fail-fast。
        if self.partition.rebalance_ms > 3_600_000 {
            return cfg(format!(
                "partition.rebalance_ms 过大({}ms,上限 3_600_000=1h):再平衡周期应远小于此",
                self.partition.rebalance_ms
            ));
        }
        if self.partition.holds_check_interval_ms == 0 {
            return cfg("partition.holds_check_interval_ms 必须 > 0".into());
        }
        if self.partition.holds_check_interval_ms > MAX_REDIS_RUNTIME_DURATION_MS {
            return cfg(format!(
                "partition.holds_check_interval_ms 过大(上限 {MAX_REDIS_RUNTIME_DURATION_MS}ms)"
            ));
        }
        if self.partition.drain_timeout_ms == 0 {
            return cfg("partition.drain_timeout_ms 必须 > 0".into());
        }
        if self.partition.drain_timeout_ms > MAX_REDIS_RUNTIME_DURATION_MS {
            return cfg(format!(
                "partition.drain_timeout_ms 过大(上限 {MAX_REDIS_RUNTIME_DURATION_MS}ms)"
            ));
        }
        if self.partition.min_idle_ms == 0 {
            return cfg("partition.min_idle_ms 必须 > 0".into());
        }
        if self.partition.min_idle_ms > MAX_REDIS_RUNTIME_DURATION_MS {
            return cfg(format!(
                "partition.min_idle_ms 过大(上限 {MAX_REDIS_RUNTIME_DURATION_MS}ms)"
            ));
        }
        if self.partition.handler_timeout_ms == 0 {
            return cfg("partition.handler_timeout_ms 必须 > 0(handler 必须有强制 timeout)".into());
        }
        if self.partition.handler_timeout_ms > MAX_REDIS_RUNTIME_DURATION_MS {
            return cfg(format!(
                "partition.handler_timeout_ms 过大(上限 {MAX_REDIS_RUNTIME_DURATION_MS}ms)"
            ));
        }
        //`max_redeliver=0` 会让每条消息首投(deliveries>=1)即判毒 → 全量进毒处置(与
        // `ProxyCfg.max_redeliver` 同纪律,retry.rs 用 `effective > max_redeliver` 判毒)。fail-fast。
        if self.partition.max_redeliver == 0 {
            return cfg("partition.max_redeliver 必须 >= 1(否则首投即判毒)".into());
        }
        //`handler_timeout_ms` 是 **per-bucket(每个 (topic,event) 桶)**,非
        // per-batch——一批 K 个桶最坏占用 K×handler_timeout。若 `drain_timeout_ms < handler_timeout_ms`,
        // 停机时只要有在途批次,优雅 drain 的绝对 deadline 必先到 → worker 被硬中断留 PEL,"优雅排水"
        // 退化为硬中断。这是软关系(不 fail-fast,部署可能有意如此),仅 warn 提示。
        if self.partition.drain_timeout_ms < self.partition.handler_timeout_ms {
            tracing::warn!(
                drain_timeout_ms = self.partition.drain_timeout_ms,
                handler_timeout_ms = self.partition.handler_timeout_ms,
                "drain_timeout_ms < handler_timeout_ms:停机时在途 handler 未结束 drain 即到点硬中断留 PEL,\
                 优雅排水会退化为硬中断(handler_timeout 还是 per-bucket,一批多桶更易触发)"
            );
        }
        if self.stream.batch_size == 0 {
            return cfg("stream.batch_size 必须 > 0(XREADGROUP COUNT 0 无意义)".into());
        }
        if self.stream.batch_size > MAX_STREAM_BATCH_SIZE {
            return cfg(format!(
                "stream.batch_size 过大({},上限 {MAX_STREAM_BATCH_SIZE}):单轮 Redis 响应和 handler 分发必须有界",
                self.stream.batch_size
            ));
        }
        if self.stream.poll_timeout_ms == 0 {
            return cfg("stream.poll_timeout_ms 必须 > 0".into());
        }
        if self.stream.poll_timeout_ms > MAX_REDIS_RUNTIME_DURATION_MS {
            return cfg(format!(
                "stream.poll_timeout_ms 过大(上限 {MAX_REDIS_RUNTIME_DURATION_MS}ms)"
            ));
        }
        if self.stream.inflight_max == 0 {
            return cfg("stream.inflight_max 必须 > 0".into());
        }
        if self.stream.inflight_max > semaphore_max {
            return cfg(format!(
                "stream.inflight_max 超过 Tokio semaphore 容量上限 {semaphore_max}"
            ));
        }
        // autoTrim 已接入 auto_trim_loop；启用时必须校验保留窗口，避免误删刚写入的 entry。
        // `auto_trim_rate_ms=0` = 禁用自动裁剪;启用(>0)时 `data_expire_ms` 必须 >0(否则保留窗为 0
        // 会把刚写入的 entry 也裁掉)。
        if self.stream.auto_trim_rate_ms > 0 && self.stream.data_expire_ms == 0 {
            return cfg(
                "stream.data_expire_ms 必须 >0(auto_trim_rate_ms>0 启用 autoTrim 时,保留窗不能为 0)".into(),
            );
        }
        if self.stream.data_expire_ms > MAX_REDIS_RUNTIME_DURATION_MS {
            return cfg(format!(
                "stream.data_expire_ms 过大(上限 {MAX_REDIS_RUNTIME_DURATION_MS}ms)"
            ));
        }
        if self.stream.auto_trim_rate_ms > MAX_REDIS_RUNTIME_DURATION_MS {
            return cfg(format!(
                "stream.auto_trim_rate_ms 过大(上限 {MAX_REDIS_RUNTIME_DURATION_MS}ms；0 表示禁用)"
            ));
        }
        // asyncDel 使用 tokio interval；0 是显式禁用，启用时限制到 1h，避免误配成极长周期后
        // 误以为 ACKed entry 会及时回收。更长的数据保留由 autoTrim 的时间窗表达。
        if self.stream.async_del_record_period_ms > 3_600_000 {
            return cfg(format!(
                "stream.async_del_record_period_ms 过大({}ms,上限 1h；0 表示禁用)",
                self.stream.async_del_record_period_ms
            ));
        }
        // Pipeline 配置 fail-fast:session_max_commands/bytes=0 会让
        // **每条命令各自 auto-flush**(退化成无批、逐条发),失去 pipeline 意义——配了却无效,比不提供更隐蔽。
        if self.pipeline.session_max_commands == 0 {
            return cfg(
                "pipeline.session_max_commands 必须 > 0(否则每条命令各自 flush,退化成无批)".into(),
            );
        }
        if self.pipeline.session_max_bytes == 0 {
            return cfg("pipeline.session_max_bytes 必须 > 0(否则每条命令各自 flush)".into());
        }
        Ok(())
    }
}

/// 命令层配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CommandCfg {
    /// 单命令响应超时 ms(0 = 不限),由 `timed()` helper 套在类型化命令 future 上。
    pub timeout_ms: u64,
    /// **连接级**响应超时 ms(0 = 不限)。直接设到 ConnectionManager/
    /// ClusterClient,**覆盖所有命令**(含 partition runtime 绕过 `timed()` 的裸 query_async)——
    /// Redis "可达但不响应"(CLIENT PAUSE/慢 Lua/TCP 黑洞)时让 future 返错而非永久 pending,
    /// 避免 coordinator 长循环卡在 Redis I/O 致 shutdown 挂死 + 分区锁泄漏。默认 30s(宽松兜底,
    /// 不误杀正常命令;需更紧可调小)。
    pub response_timeout_ms: u64,
}

impl Default for CommandCfg {
    /// 业务作用：构造命令层默认配置。
    ///
    /// 默认不限制单命令业务超时,但给连接级响应超时 30s 兜底,避免 Redis 可达但不响应时 future 永久挂起。
    fn default() -> Self {
        Self {
            timeout_ms: 0,
            response_timeout_ms: 30_000,
        }
    }
}

/// 保存 PipelineCfg 配置项；用于把外部参数集中传入运行时。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PipelineCfg {
    /// 单个 PipelineSession 的**滚动 auto-flush 阈值**(对齐 原实现 pipelineLength=1000):当前批到此条数即
    /// seal 后台发出、会话续接,**不报错**(第 1001 条无缝接续)。
    pub session_max_commands: usize,
    /// 单批累计参数字节阈值:加上本命令会超此值即先 auto-flush 当前批(防单批超大 value 占满内存)。
    pub session_max_bytes: usize,
    /// **pipeline 专用连接 lane**
    /// `true`(默认)= pipeline 流量走**独立多路复用连接**,与 direct/control/lock/partition fencing 隔离,
    /// 防大批量 pipeline 响应在共享连接上头阻塞、拖慢其它命令尾延迟。**惰性创建**(首次 pipeline 才建连,
    /// 不用 pipeline 的服务零成本)。`false` = 复用共享连接(旧行为)。
    pub dedicated_conn: bool,
}
impl Default for PipelineCfg {
    /// 业务作用：构造 pipeline 默认配置。
    ///
    /// 默认 1000 条或 4MiB 触发自动滚动 flush,并启用独立 pipeline 连接 lane,避免大批量响应阻塞普通命令。
    fn default() -> Self {
        Self {
            session_max_commands: 1000,
            session_max_bytes: 4 * 1024 * 1024,
            dedicated_conn: true,
        }
    }
}

/// 保存 LockCfg 配置项；用于把外部参数集中传入运行时。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LockCfg {
    /// 锁 key 前缀(原实现 默认 "DISTRIBUTED-LOCK:";三级回退在 原实现 侧,Rust 单层显式)。
    pub prefix: String,
    /// 锁过期 ms(原实现 默认 30000);看门狗每 lease/3 续期。
    pub lease_ms: u64,
}
impl Default for LockCfg {
    /// 业务作用：构造分布式锁默认配置。
    ///
    /// 默认前缀兼容原实现,lease 为 30s；看门狗按 lease/3 续租,因此业务锁默认能覆盖常见短事务。
    fn default() -> Self {
        Self {
            prefix: "DISTRIBUTED-LOCK:".into(),
            lease_ms: 30_000,
        }
    }
}

/// stream 配置；默认值在类型层统一固化，避免不同入口发生漂移。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StreamCfg {
    /// 托管模式 = 冷流再 poll 间隔(NOBLOCK);仅非 group XREAD 才是 BLOCK 参数。原实现 默认 500。
    /// **已接线**:冷流 Backoff / coordinator loop 等待。
    pub poll_timeout_ms: u64,
    /// XREADGROUP COUNT。**已接线**。
    pub batch_size: usize,
    /// 自动裁剪保留窗，既有默认值为 1 小时。leader 每 `auto_trim_rate_ms`
    /// 对各分区 stream `XTRIM MINID ~ {now-本值}`。`auto_trim_rate_ms=0` 时本值不生效(裁剪禁用)。
    pub data_expire_ms: u64,
    /// 本地 XTRIM 周期，既有默认值为 60s；**0 表示禁用自动裁剪**。
    pub auto_trim_rate_ms: u64,
    /// ACK 成功后的异步 XDEL 批删周期。单 owner + 有界通道，队列满时反压消费；删除失败留到
    /// 下一周期，停机在业务 drain 后做末次 flush。**0 = 禁用**，此时 entry 留在 stream；
    /// 只有 autoTrim 同时启用时才会按其保留窗回收。
    pub async_del_record_period_ms: u64,
    /// 全局在飞批次预算上界。**已接线**:= coordinator budget Semaphore 容量。
    pub inflight_max: usize,
}
impl Default for StreamCfg {
    /// 业务作用：构造 stream 消费默认配置。
    ///
    /// 默认以 500ms 冷流轮询、100 条批量和 1 小时数据保留窗运行,并设置全局在飞批次预算。
    fn default() -> Self {
        Self {
            poll_timeout_ms: 500,
            batch_size: 100,
            data_expire_ms: 60 * 60 * 1000,
            auto_trim_rate_ms: 60_000,
            async_del_record_period_ms: 5_000,
            inflight_max: 1000,
        }
    }
}

/// 毒消息处置策略；Drop、Park 与 Dlq 均通过显式状态和 planned-ID reservations 协议收敛。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PoisonPolicy {
    /// 丢弃:fenced ACK 清 PEL + error 日志(消息不可恢复——显式选择才允许)。
    Drop,
    /// 停拉隔离:消息转存 quarantine HASH + disposition marker,该分区停止消费,
    /// 等管理 API resume/drop 显式处置(分区顺序让位于人工介入)。
    Park,
    /// 死信:转存后立即按 planned-ID 协议发布到 DLQ stream({prefix}:dlq),源 PEL 清空,
    /// 分区**继续消费**（先 Park 转存再自动执行 dlq 处置；中途崩溃自然回退为
    /// Parked 可管理态,无半完成状态)。
    Dlq,
}

/// partition 配置；所有入口共享同一组默认值。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PartitionCfg {
    /// 是否启用 partition 消费。
    pub enabled: bool,
    /// 默认消费组名称。
    pub default_group: String,
    /// 分区数量。
    pub count: u32,
    /// 重平衡周期毫秒数。
    pub rebalance_ms: u64,
    /// PEL 消息被视为可接管的最小空闲毫秒数。
    pub min_idle_ms: u64,
    /// 持有分区检查周期毫秒数。
    pub holds_check_interval_ms: u64,
    /// 停机 drain 等待毫秒数。
    pub drain_timeout_ms: u64,
    /// 同一批次连续失败的重投上限;超过即触发 poison_policy。
    pub max_redeliver: u32,
    /// handler 强制 timeout ms(文档 handler 契约:超时一律按失败留 PEL)。
    /// ⚠ **per-bucket(每个 (topic,event) 桶各套一次),非 per-batch**:一批含
    /// K 个桶时最坏占用 ≈K×handler_timeout。配 `drain_timeout_ms` 时务必 ≥ 本值,否则停机优雅 drain
    /// 会被在途 handler 拖到硬中断(validate 已 warn)。
    /// 注意:对**纯 CPU、不 yield** 的 handler 无效(async 无法中断不让出的同步块),那类
    /// 业务须自行 spawn_blocking / 自带超时;本 timeout 兜住会 await 的慢 handler。
    pub handler_timeout_ms: u64,
    /// 毒消息策略(默认 Park——fail-closed:停拉隔离等运维处置,**不自动删数据**;
    ///文档 V2 默认即 Park,Drop 是显式选择的不可恢复丢弃)。
    pub poison_policy: PoisonPolicy,
    /// **隔离组**(对照 原实现 `partition.groups.{逻辑名}`):把高频/重 topic 隔离到独立 stream 组,
    /// 与默认组及其它组**互不阻塞消费**(高频/低频、高高频/高低频隔离)。空 = 仅默认组(向后兼容)。
    /// key = 逻辑短名(不含 default_group 前缀;不得为空/含 `{`/`}`)。实际 stream 前缀 =
    /// `{default_group}:{逻辑名}`(对照 原实现 streamPrefix)。
    #[serde(default)]
    pub groups: std::collections::HashMap<String, PartitionGroupCfg>,
}
impl Default for PartitionCfg {
    /// 业务作用：构造 partition 模式默认配置。
    ///
    /// 默认关闭 partition,但保留原实现的默认组名、64 分区、10s 再平衡和 Park 毒消息策略。
    fn default() -> Self {
        Self {
            enabled: false,
            default_group: "SINGLE-CONSUME".into(),
            count: 64,
            rebalance_ms: 10_000,
            min_idle_ms: 30_000,
            holds_check_interval_ms: 5_000,
            // 停机预算必须长于默认的单桶处理超时，否则默认配置下每个仍在处理慢任务的实例都会被
            // 强制中断，无法兑现先排空再退出的生命周期语义。
            drain_timeout_ms: 35_000,
            max_redeliver: 5,
            handler_timeout_ms: 30_000,
            poison_policy: PoisonPolicy::Park,
            groups: std::collections::HashMap::new(),
        }
    }
}

/// 单个隔离组配置(对照 原实现 `PartitionGroup` 的 yml `partition.groups.{逻辑名}`)。
/// 路由(topics)+ 分区数(count)+ **per-group 运行时参数覆盖**(对照 原实现 per-group override):每个
/// `Option` 字段 `None` = 继承父级 `PartitionCfg` / 全局 `StreamCfg`,`Some` = 覆盖。让高频组用更大 batch、
/// 更短 poll/rebalance,慢任务组用更长 drain/min_idle 等。**覆盖值进 protocol marker**(异构运行参数 fail-closed)。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct PartitionGroupCfg {
    /// 路由到本隔离组的 topic 列表;空 = 逻辑组名本身即唯一 topic(对照 原实现 `isolate(topic, topic, count)`)。
    pub topics: Vec<String>,
    /// 本组分区数;0 = 继承 `partition.count`。
    pub count: u32,
    // ── per-group 运行时覆盖(None = 继承父级 partition.*;对照 原实现 PartitionGroup)──
    /// 再平衡周期 ms(继承 `partition.rebalance_ms`)。
    pub rebalance_ms: Option<u64>,
    /// XAUTOCLAIM min-idle ms(继承 `partition.min_idle_ms`)。
    pub min_idle_ms: Option<u64>,
    /// holds 自检最小间隔 ms(继承 `partition.holds_check_interval_ms`)。
    pub holds_check_interval_ms: Option<u64>,
    /// 停机 drain 超时 ms(继承 `partition.drain_timeout_ms`)。
    pub drain_timeout_ms: Option<u64>,
    /// handler 强制超时 ms(继承 `partition.handler_timeout_ms`)。
    pub handler_timeout_ms: Option<u64>,
    /// 重投上限(继承 `partition.max_redeliver`)。
    pub max_redeliver: Option<u32>,
    /// 毒消息策略(继承 `partition.poison_policy`)。
    pub poison_policy: Option<PoisonPolicy>,
    // ── per-group StreamCfg 覆盖(None = 继承全局 stream.*)──
    /// XREADGROUP COUNT(继承 `stream.batch_size`)——高频组常需更大。
    pub batch_size: Option<usize>,
    /// 冷流再 poll 间隔 ms(继承 `stream.poll_timeout_ms`)。
    pub poll_timeout_ms: Option<u64>,
    /// 全局在飞批次预算(继承 `stream.inflight_max`)。
    pub inflight_max: Option<usize>,
}

impl PartitionGroupCfg {
    /// 业务作用：resolve 本组的 `PartitionCfg`(per-group 覆盖 → 父级;`count` 由调用方传 resolved 值)。
    /// 用于 `GroupRuntime`:`cfg.groups` 清空(per-group 无嵌套组),`enabled/default_group` 沿父级(runtime 不读)。
    pub(crate) fn resolved_partition(&self, parent: &PartitionCfg, count: u32) -> PartitionCfg {
        PartitionCfg {
            enabled: parent.enabled,
            default_group: parent.default_group.clone(),
            count,
            rebalance_ms: self.rebalance_ms.unwrap_or(parent.rebalance_ms),
            min_idle_ms: self.min_idle_ms.unwrap_or(parent.min_idle_ms),
            holds_check_interval_ms: self
                .holds_check_interval_ms
                .unwrap_or(parent.holds_check_interval_ms),
            drain_timeout_ms: self.drain_timeout_ms.unwrap_or(parent.drain_timeout_ms),
            max_redeliver: self.max_redeliver.unwrap_or(parent.max_redeliver),
            handler_timeout_ms: self.handler_timeout_ms.unwrap_or(parent.handler_timeout_ms),
            poison_policy: self.poison_policy.unwrap_or(parent.poison_policy),
            groups: std::collections::HashMap::new(),
        }
    }

    /// 业务作用：resolve 本组的 `StreamCfg`(per-group 覆盖 → 全局 stream)。
    pub(crate) fn resolved_stream(&self, global: &StreamCfg) -> StreamCfg {
        StreamCfg {
            poll_timeout_ms: self.poll_timeout_ms.unwrap_or(global.poll_timeout_ms),
            batch_size: self.batch_size.unwrap_or(global.batch_size),
            data_expire_ms: global.data_expire_ms,
            auto_trim_rate_ms: global.auto_trim_rate_ms,
            async_del_record_period_ms: global.async_del_record_period_ms,
            inflight_max: self.inflight_max.unwrap_or(global.inflight_max),
        }
    }
}
