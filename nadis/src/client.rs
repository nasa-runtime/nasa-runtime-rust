// Redis 客户端负责连接、协议标记校验与组件装配。
// connect 建立连接并校验 namespace 内的 profile 与命名配置，一致后才返回可用客户端。
// 锁、partition 与 search 由调用方显式创建，不因连接成功就自动启动各自后台任务。
// 协议标记由遵循该约定的客户端写入和校验，不能约束绕过标记的其它客户端。
// 不同协议的 namespace 与 group 必须由部署配置隔离，避免误连破坏数据布局或时钟边界。

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use redis::aio::ConnectionManager;
use redis::AsyncCommands;
#[cfg(feature = "job")]
use redis::IntoConnectionInfo;

use crate::config::{CompatibilityProfile, RedisConfig};
use crate::error::{NasaRedisError, Result};

/// 传输抽象：单节点使用 `ConnectionManager`，Cluster 使用 `cluster_async::ClusterConnection`。
/// 两者均 impl `ConnectionLike`(故 `AsyncCommands`/`query_async` 在 `Conn` 上通用),Clone 廉价(多路
/// 复用句柄)。Cluster 变体**跟随 MOVED/ASK**,单键命令按 slot 路由到正确节点;多键 Lua/MULTI 仍要求
/// 同槽 key（否则 CROSSSLOT）；partition 多键脚本由 group 级 relayout 保证同槽。
#[derive(Clone)]
pub enum Conn {
    /// 单点 Redis 连接管理器。
    Single(ConnectionManager),
    /// Redis Cluster 异步连接,可跟随 MOVED/ASK 重定向。
    Cluster(redis::cluster_async::ClusterConnection),
}

impl redis::aio::ConnectionLike for Conn {
    /// 业务作用：将单条 Redis 命令交给已选择的单节点或集群传输，不额外重试业务操作。
    ///
    /// 参数说明：
    /// - `cmd`：待发送命令及参数，集群路由由底层连接根据命令键决定。
    ///
    /// 返回：底层响应值或传输、协议及服务器错误；错误不证明命令未在远端执行。
    fn req_packed_command<'a>(
        &'a mut self,
        cmd: &'a redis::Cmd,
    ) -> redis::RedisFuture<'a, redis::Value> {
        match self {
            Conn::Single(c) => c.req_packed_command(cmd),
            Conn::Cluster(c) => c.req_packed_command(cmd),
        }
    }

    /// 业务作用：将命令管线交给对应传输，并保留底层响应窗口语义。
    ///
    /// 参数说明：
    /// - `cmd`：包含待发送命令的完整管线。
    /// - `offset`：响应中跳过的前缀数量，不是业务列表或键的偏移量。
    /// - `count`：本次需要保留的响应数量。
    ///
    /// 返回：指定窗口的响应集合或底层错误；部分命令可能已经执行，不自动保证批次原子性。
    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> redis::RedisFuture<'a, Vec<redis::Value>> {
        match self {
            Conn::Single(c) => c.req_packed_commands(cmd, offset, count),
            Conn::Cluster(c) => c.req_packed_commands(cmd, offset, count),
        }
    }

    /// 业务作用：返回当前 Redis 连接配置的逻辑库编号。
    ///
    /// 参数说明：无。
    ///
    /// 返回：底层传输记录的逻辑库编号；此操作不查询服务器，也不证明连接当前可用。
    fn get_db(&self) -> i64 {
        match self {
            Conn::Single(c) => c.get_db(),
            Conn::Cluster(c) => c.get_db(),
        }
    }
}

/// 业务作用：为同一 Redis 命名空间定位共享协议标记，使连接时能检查数据布局兼容性。
///
/// 参数说明：
/// - `namespace`：已通过配置校验的业务命名空间。
///
/// 返回：固定前缀加命名空间形成的标记键，不访问 Redis。
fn marker_key(namespace: &str) -> String {
    format!("nasa:protocol:{namespace}")
}

/// 业务作用：按固定顺序生成协议与命名配置的比较文本，避免仅凭短摘要判断配置一致。
///
/// 参数说明：
/// - `cfg`：已校验的 Redis 配置，包含协议、锁前缀、默认组及分区布局。
///
/// 返回：完整配置比较文本；存在隔离组时附加继承解析后的分组 JSON，不执行远端标记写入。
fn naming_config_canonical(cfg: &RedisConfig) -> String {
    let base = format!(
        "v1|profile={}|lock.prefix={}|partition.default_group={}|partition.count={}",
        cfg.profile.wire_name(),
        cfg.lock.prefix,
        cfg.partition.default_group,
        cfg.partition.count
    );
    //**多组 marker**——`partition.groups` 直接决定 topic 路由 / stream 前缀 / consumer
    // group 名 / lock·fence·disposition·DLQ key 命名空间 / cluster hash-tag,异构 groups 配置必须 fail-closed,
    // 否则同 namespace 一部分节点走隔离组、一部分走默认组 → 同 topic 物理队列分裂(高频/低频隔离最怕)。
    // 空 groups 使用基础协议格式；非空时纳入所有继承后的组参数，防止同名 topic 被异构布局拆分。
    if cfg.partition.groups.is_empty() {
        base
    } else {
        format!("{base}|partition.groups={}", canonical_groups(cfg))
    }
}

/// 业务作用：稳定序列化多组配置(label `partition.groups`)。**结构化 JSON**(非手拼分隔符,含 `,`/`;`
/// 的 topic 会注入碰撞 fail-open;JSON 转义单射)。形态 `{default:<runtime>, groups:{逻辑名:<topics+runtime>}}`:
/// - **`default`**:默认组 resolved runtime(= 父级 PartitionCfg + 全局 StreamCfg;默认组也消费
///   所有未隔离 topic,其 rebalance/min_idle/drain/batch 等不一致同样致跨节点 liveness/背压/drain 判定分裂,
///   故默认组运行参数也必须进入 marker)。
/// - **`groups`**:各隔离组 resolved(topics 排序 + 空 topics 归一逻辑名 + count/runtime 取 override→父级/全局)。
///
/// 所有字段 resolved(`0 继承`与显式父级值同 canonical,不误 fail-closed);BTreeMap 排序键稳定。
///
/// 参数说明：
/// - `cfg`：包含父级分区参数、默认 Stream 参数及各隔离组覆盖值的配置。
///
/// 返回：按组名和 topic 排序、展开继承值的 JSON 文本；相同有效组配置得到相同文本。
fn canonical_groups(cfg: &RedisConfig) -> String {
    let p = &cfg.partition;
    let s = &cfg.stream;
    /// 参与协议标记比较的有效运行参数，不持有任务或连接。
    #[derive(serde::Serialize)]
    struct CanonRuntime {
        /// 本组物理分区数量，决定键空间布局。
        count: u32,
        /// 所有权再均衡周期，单位毫秒。
        rebalance_ms: u64,
        /// 待处理消息被回收前的最小空闲时间，单位毫秒。
        min_idle_ms: u64,
        /// 持有权复验周期，单位毫秒。
        holds_check_interval_ms: u64,
        /// 撤销所有权后的本地排空期限，单位毫秒。
        drain_timeout_ms: u64,
        /// 单条处理任务的等待上限，单位毫秒。
        handler_timeout_ms: u64,
        /// 达到毒消息处置前允许的重投次数。
        max_redeliver: u32,
        /// 重投预算耗尽后的消息处置策略。
        poison_policy: crate::config::PoisonPolicy,
        /// 每次读取允许的消息数量。
        batch_size: usize,
        /// 单次阻塞读取的有效毫秒数。
        poll_timeout_ms: u64,
        /// 本组允许的在途消息上限。
        inflight_max: usize,
    }
    /// 承载规范化后的分组名；用于统一 Redis 分组标记的比较口径。
    #[derive(serde::Serialize)]
    struct CanonGroup<'a> {
        /// 按名称排序的路由主题，空配置已解析为逻辑组名。
        topics: Vec<&'a str>,
        /// 覆盖和继承均已展开的组参数，与主题一并比较。
        #[serde(flatten)]
        rt: CanonRuntime,
    }
    /// 承载规范化后的分片名；用于统一 Redis 分片标记的比较口径。
    #[derive(serde::Serialize)]
    struct CanonPartition<'a> {
        /// 未被隔离组接管主题所使用的默认组参数。
        default: CanonRuntime,
        /// 以逻辑组名排序的隔离组布局。
        groups: std::collections::BTreeMap<&'a str, CanonGroup<'a>>,
    }
    // 默认组 resolved runtime = 父级 partition + 全局 stream(默认组无 per-group 覆盖)。
    let default = CanonRuntime {
        count: p.count,
        rebalance_ms: p.rebalance_ms,
        min_idle_ms: p.min_idle_ms,
        holds_check_interval_ms: p.holds_check_interval_ms,
        drain_timeout_ms: p.drain_timeout_ms,
        handler_timeout_ms: p.handler_timeout_ms,
        max_redeliver: p.max_redeliver,
        poison_policy: p.poison_policy,
        batch_size: s.batch_size,
        // validate 已拒绝 0；这里保留 `.max(1)` 只是防御性地确保 canonical 值与运行时有效值一致。
        poll_timeout_ms: s.poll_timeout_ms.max(1),
        inflight_max: s.inflight_max,
    };
    let mut groups: std::collections::BTreeMap<&str, CanonGroup> =
        std::collections::BTreeMap::new();
    for (logical, g) in &p.groups {
        let mut topics: Vec<&str> = if g.topics.is_empty() {
            vec![logical.as_str()] // 空 topics → 逻辑名即 topic(对照路由/validate)
        } else {
            g.topics.iter().map(|s| s.as_str()).collect()
        };
        topics.sort_unstable();
        groups.insert(
            logical.as_str(),
            CanonGroup {
                topics,
                rt: CanonRuntime {
                    count: if g.count > 0 {
                        g.count
                    } else {
                        p.count
                    },
                    rebalance_ms: g.rebalance_ms.unwrap_or(p.rebalance_ms),
                    min_idle_ms: g.min_idle_ms.unwrap_or(p.min_idle_ms),
                    holds_check_interval_ms: g
                        .holds_check_interval_ms
                        .unwrap_or(p.holds_check_interval_ms),
                    drain_timeout_ms: g.drain_timeout_ms.unwrap_or(p.drain_timeout_ms),
                    handler_timeout_ms: g.handler_timeout_ms.unwrap_or(p.handler_timeout_ms),
                    max_redeliver: g.max_redeliver.unwrap_or(p.max_redeliver),
                    poison_policy: g.poison_policy.unwrap_or(p.poison_policy),
                    batch_size: g.batch_size.unwrap_or(s.batch_size),
                    poll_timeout_ms: g.poll_timeout_ms.unwrap_or(s.poll_timeout_ms).max(1),
                    inflight_max: g.inflight_max.unwrap_or(s.inflight_max),
                },
            },
        );
    }
    serde_json::to_string(&CanonPartition {
        default,
        groups,
    })
    .expect("canonical partition json 序列化不应失败")
}

/// 业务作用：按兼容标记的协议名与 CRC16 摘要语义检查当前命名配置。
///
/// 参数说明：
/// - `existing`：从协议标记键读取的 JSON 文本。
/// - `cfg`：本节点已校验的配置，用于生成协议名与命名配置摘要。
///
/// 返回：协议名与摘要均匹配时为真；格式非法、字段缺失或不一致时为假，不改写标记。
/// CRC16 仅提供兼容格式的比较能力，不能提供完整文本比较或抗碰撞身份保证。
fn legacy_marker_matches(existing: &str, cfg: &RedisConfig) -> bool {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(existing) else {
        return false;
    };
    match (
        v.get("profile").and_then(|x| x.as_str()),
        v.get("naming_config_hash").and_then(|x| x.as_str()),
    ) {
        (Some(prof), Some(hash)) => {
            // profile 必须与本节点持久 wire 名相等;RustV2 与 LegacyV1 严格区分。
            if prof != cfg.profile.wire_name() {
                return false;
            }
            let canon = naming_config_canonical(cfg);
            let crc = |s: &str| format!("{:04x}", crate::keytag::crc16(s.as_bytes()));
            hash == crc(&canon)
        }
        _ => false,
    }
}

/// 业务作用：构造待写入或比较的协议标记，使用稳定协议名和完整命名配置文本。
///
/// 参数说明：
/// - `cfg`：本节点通过校验的协议及命名配置。
///
/// 返回：含 `profile` 和 `naming_config` 的 JSON 文本，字符串内容按 JSON 规则转义；不访问 Redis。
fn marker_payload(cfg: &RedisConfig) -> String {
    serde_json::json!({
        "profile": cfg.profile.wire_name(),
        "naming_config": naming_config_canonical(cfg),
    })
    .to_string()
}

/// 业务作用：比较协议标记内容，避免仅因 JSON 对象字段顺序不同拒绝等价配置。
///
/// 参数说明：
/// - `existing`：Redis 中已存在的协议标记文本。
/// - `payload`：本节点配置生成的完整协议标记文本。
///
/// 返回：双方可解析时按 JSON 值比较；任一解析失败时仅接受原始文本相同。
/// 本方法不验证 CRC16 兼容标记，调用方须单独裁决该格式。
fn marker_semantically_equal(existing: &str, payload: &str) -> bool {
    match (
        serde_json::from_str::<serde_json::Value>(existing),
        serde_json::from_str::<serde_json::Value>(payload),
    ) {
        (Ok(a), Ok(b)) => a == b,
        _ => existing == payload,
    }
}

/// Redis 客户端。
pub struct RedisClient {
    /// 建连前校验并冻结的连接、协议和各子系统参数。
    cfg: RedisConfig,
    /// 普通命令共享传输；底层重连不证明已发命令的执行结果，调用方须处理断链后的结果不确定性。
    conn: Conn,
    /// 底层 client:派生专用连接(Pub/Sub、阻塞命令)用(cluster 下为 seed 节点;classic pubsub 跨节点)。
    raw: redis::Client,
    /// 目标是否为 Cluster 模式；partition 多键脚本依赖 group 级 relayout 保证同槽。
    is_cluster: bool,
    /// **pipeline 专用 lane**:独立多路复用连接,与 direct/control/lock 隔离,
    /// 防大批量 pipeline 头阻塞共享连接。**惰性**——首次 pipeline 才建连(`pipeline.dedicated_conn=true` 时)。
    pipe_cell: tokio::sync::OnceCell<Conn>,
    /// Job 状态脚本专用 lane：与普通命令、Pipeline 和阻塞读取隔离，避免业务流量阻塞租约与完成提交。
    #[cfg(feature = "job")]
    job_control_cell: tokio::sync::OnceCell<Conn>,
    /// Job 心跳专用 lane：传输重建与截止内重发不能让其它状态脚本继承同一断链结局。
    #[cfg(feature = "job")]
    job_heartbeat_cell: tokio::sync::OnceCell<Conn>,
    /// 当前 Job source generation 的本地指标弱引用；运行时换代可替换，客户端不会反向延长其生命周期。
    #[cfg(feature = "job")]
    job_metrics: RwLock<Option<std::sync::Weak<crate::job::metrics::JobMetrics>>>,
    /// nonce 幂等计数运行时:惰性构造,只用基础命令的应用不承担布局解析与能力探测成本;
    /// 首次 `*_idempotent` 调用才解析共享 marker(见连接角色表的幂等计数零成本不变量)。
    pub(crate) idempotent:
        std::sync::OnceLock<crate::idempotent::runtime::IdempotentCounterRuntime>,
}

/// 业务作用：限制一次建连阶段的总等待，覆盖该阶段的解析、握手及拓扑建立。
///
/// 参数说明：
/// - `fut`：返回 Redis 连接结果的异步操作。
/// - `ms`：该阶段最大等待毫秒数，零表示不设置本层截止时间。
/// - `what`：用于超时归因的固定操作名称，不应包含凭据。
///
/// 返回：操作完成时保留其结果；截止时间到达时取消本地等待并返回 Redis I/O 类错误。
async fn await_with_deadline<F, T>(
    fut: F,
    ms: u64,
    what: &str,
) -> std::result::Result<T, redis::RedisError>
where
    F: std::future::Future<Output = std::result::Result<T, redis::RedisError>>,
{
    if ms == 0 {
        return fut.await;
    }
    match tokio::time::timeout(Duration::from_millis(ms), fut).await {
        Ok(r) => r,
        Err(_) => Err(redis::RedisError::from((
            redis::ErrorKind::Io,
            "建连超时",
            format!("{what} 超过 {ms}ms 未就绪(Redis 不可达?)"),
        ))),
    }
}

/// 业务作用：从拓扑响应取得唯一且明确的集群模式，拒绝凭缺失字段推断单节点连接。
///
/// 参数说明：
/// - `info`：`INFO cluster` 的完整文本响应，允许其它信息行及 CRLF 或 LF 换行。
///
/// 返回：唯一 `cluster_enabled` 字段为 `0` 或 `1` 时返回对应模式；缺失、重复或值非法时
/// 返回固定解析错误，不携带响应正文，也不允许继续创建协议标记。
fn parse_cluster_mode(info: &str) -> std::result::Result<bool, redis::RedisError> {
    let mut modes = info
        .lines()
        .filter_map(|line| line.strip_prefix("cluster_enabled:"));
    // 重复值即使相同也不是唯一拓扑证据，不能按首值或子串匹配选择业务传输。
    match (modes.next(), modes.next()) {
        (Some("0"), None) => Ok(false),
        (Some("1"), None) => Ok(true),
        _ => Err(redis::RedisError::from((
            redis::ErrorKind::Parse,
            "INFO cluster requires exactly one cluster_enabled field with value 0 or 1",
        ))),
    }
}

/// 业务作用：把某建连阶段的底层 redis 错误包装成带阶段与脱敏 endpoint 的 [`NasaRedisError::ConnectProbe`]。
///
/// 参数说明：
/// - `stage`: 失败阶段词(`connect`/`topology`/`cluster-discovery`/`protocol-marker`)。
/// - `cfg`: 当前 Redis 配置,取其 `safe_endpoint()` 作定位信息(不含凭据)。
/// - `source`: 底层 redis 错误,原样保留为错误链根因。
///
/// 返回：携带固定阶段与安全端点的错误；底层错误链保持原文，上层对外输出时仍须控制披露范围。
fn connect_probe(
    stage: &'static str,
    cfg: &RedisConfig,
    source: redis::RedisError,
) -> NasaRedisError {
    NasaRedisError::ConnectProbe {
        stage,
        endpoint: cfg.safe_endpoint(),
        source,
    }
}

impl RedisClient {
    /// 业务作用：校验配置、建立 Redis 传输并核对共享协议标记，完成后才交付客户端。
    ///
    /// 参数说明：
    /// - `cfg`: Redis 连接、命名空间、profile 和各子模块运行参数。
    ///
    /// 返回：协议标记新建或兼容校验通过后返回共享客户端；配置非法、连接失败或标记冲突时拒绝。
    /// 成功不启动消费、锁看门狗或 Job 任务；阶段超时按连接配置分别生效，并非整个启动过程共享一个截止点。
    pub async fn connect(cfg: RedisConfig) -> Result<Arc<RedisClient>> {
        cfg.validate()?;
        let raw =
            redis::Client::open(cfg.url.as_str()).map_err(|e| connect_probe("connect", &cfg, e))?;
        //连接级 response_timeout——覆盖所有命令(含 partition runtime 绕过
        // `timed()` 的裸调用),Redis 黑洞时返错而非永久 pending。0 = 不限(escape hatch)。
        let resp_timeout = cfg.command.response_timeout_ms;
        //**建连本身**也要有 deadline——response_timeout 只覆盖已建连后的命令,
        // 建连 / `INFO cluster` 探测 / cluster `get_async_connection()` 若无超时,Redis 不可达/TCP 黑洞时
        // `connect()` 永久 pending、**启动 fail-fast 失效**。故:① builder 设 connection_timeout;
        // ② 整个建连过程再套 `tokio::time::timeout` 兜底(覆盖 DNS/builder 未覆盖处)。0 = 不限(escape hatch)。
        let mgr_cfg = {
            let mut c = redis::aio::ConnectionManagerConfig::new();
            if resp_timeout > 0 {
                let d = Duration::from_millis(resp_timeout);
                c = c
                    .set_response_timeout(Some(d))
                    .set_connection_timeout(Some(d));
            }
            c
        };
        // 先用单节点 ConnectionManager 探测拓扑(INFO cluster);再据此选 transport。
        let mut bootstrap = await_with_deadline(
            ConnectionManager::new_with_config(raw.clone(), mgr_cfg),
            resp_timeout,
            "ConnectionManager 建连",
        )
        .await
        .map_err(|e| connect_probe("connect", &cfg, e))?;

        //`cluster_enabled:1` 在 **`INFO`(cluster 段)**(不在 `CLUSTER INFO`)。
        // 检测到 Cluster 后必须使用 `cluster_async::ClusterConnection` 跟随 MOVED/ASK。
        // standalone 也须通过 INFO 明确返回 cluster_enabled:0；连接异常或 ACL 拒绝不构成拓扑证据。
        // 探测重试三次仍失败时拒绝启动，避免给 Cluster 建立无法处理 MOVED 的 Single 连接。
        let is_cluster = {
            let mut last_err = None;
            let mut detected = None;
            for attempt in 0..3 {
                match redis::cmd("INFO")
                    .arg("cluster")
                    .query_async::<String>(&mut bootstrap)
                    .await
                {
                    Ok(info) => {
                        // 只有明确拓扑证据才能选择连接模式；协议响应异常时在任何标记写入前拒绝。
                        detected = Some(
                            parse_cluster_mode(&info)
                                .map_err(|error| connect_probe("topology", &cfg, error))?,
                        );
                        break;
                    }
                    Err(e) => {
                        tracing::warn!(attempt, err = %e, "`INFO cluster` 失败(连接故障),重试");
                        last_err = Some(e);
                        if resp_timeout > 0 {
                            tokio::time::sleep(Duration::from_millis(
                                (resp_timeout / 3).clamp(50, 500),
                            ))
                            .await;
                        }
                    }
                }
            }
            match detected {
                Some(b) => b,
                None => {
                    // fail-closed:重试耗尽仍失败 = 真·连接故障/节点不可达/ACL 禁 INFO,绝不静默降级
                    // standalone(否则目标实为 cluster 时会建出不跟 MOVED 的 Single → 运行期 MOVED 风暴)。
                    //:保留底层错误链 + `topology` 阶段,供启动诊断定位,不再把根因 to_string 后丢弃 source。
                    return Err(match last_err {
                        Some(e) => connect_probe("topology", &cfg, e),
                        None => NasaRedisError::Config(
                            "`INFO cluster` 拓扑探测失败且无底层错误可用(不应发生)".into(),
                        ),
                    });
                }
            }
        };
        let mut conn = if is_cluster {
            // Cluster transport 由 seed URL 触发拓扑发现，并跟随 MOVED/ASK 路由。
            // 同样设连接级 response_timeout(覆盖 cluster 下裸命令)。
            let mut builder = redis::cluster::ClusterClient::builder(vec![cfg.url.as_str()]);
            if resp_timeout > 0 {
                let d = Duration::from_millis(resp_timeout);
                builder = builder.response_timeout(d).connection_timeout(d);
            }
            let cc = builder
                .build()
                .map_err(|e| connect_probe("cluster-discovery", &cfg, e))?;
            tracing::info!(namespace = %cfg.namespace, "目标 Redis 为 Cluster 模式,启用 cluster_async transport(跟随 MOVED/ASK)");
            // cluster 建连也套整体 deadline(get_async_connection 触发拓扑发现,可能久挂)
            Conn::Cluster(
                await_with_deadline(cc.get_async_connection(), resp_timeout, "cluster 建连")
                    .await
                    .map_err(|e| connect_probe("cluster-discovery", &cfg, e))?,
            )
        } else {
            Conn::Single(bootstrap)
        };

        // ── 协议标记:SET NX 创建,已存在则校验──
        // 用 serde_json 构造 payload:naming_config 完整串里若含引号/反斜杠(prefix/group 自定义)
        // 也安全转义。比对走**语义相等**(parse 成 Value 比 key-value 集合,不依赖键序)——
        // serde_json 开 preserve_order 后键序 = 声明序,不再字母序,故不能再做裸字符串相等。
        let key = marker_key(&cfg.namespace);
        let payload = marker_payload(&cfg);
        let created: bool = redis::cmd("SET")
            .arg(&key)
            .arg(&payload)
            .arg("NX")
            .query_async::<Option<String>>(&mut conn)
            .await
            .map_err(|e| connect_probe("protocol-marker", &cfg, e))?
            .is_some();
        if !created {
            let existing: String = conn
                .get(&key)
                .await
                .map_err(|e| connect_probe("protocol-marker", &cfg, e))?;
            // 语义相等(新格式,键序无关)或旧格式 CRC16 校验通过(迁移兼容)即放行
            if !marker_semantically_equal(&existing, &payload)
                && !legacy_marker_matches(&existing, &cfg)
            {
                return Err(NasaRedisError::ProtocolMarker(format!(
                    "namespace={} 既有标记 {existing} 与本节点 {payload} 不一致——profile 或命名配置不符,拒绝启动",
                    cfg.namespace
                )));
            }
        } else {
            tracing::info!(namespace = %cfg.namespace, profile = ?cfg.profile, "协议标记已创建");
        }

        Ok(Arc::new(RedisClient {
            cfg,
            conn,
            raw,
            is_cluster,
            pipe_cell: tokio::sync::OnceCell::new(),
            #[cfg(feature = "job")]
            job_control_cell: tokio::sync::OnceCell::new(),
            #[cfg(feature = "job")]
            job_heartbeat_cell: tokio::sync::OnceCell::new(),
            #[cfg(feature = "job")]
            job_metrics: RwLock::new(None),
            idempotent: std::sync::OnceLock::new(),
        }))
    }

    /// 业务作用：返回目标是否为 Cluster 模式；partition 多键脚本通过 group 级 relayout 保证同槽。
    ///
    /// 参数说明：无。
    ///
    /// 返回：建连阶段确定并冻结的拓扑模式，不重新探测服务器或证明当前集群健康。
    pub fn is_cluster(&self) -> bool {
        self.is_cluster
    }

    /// 业务作用：返回当前连接使用的兼容性 profile。
    ///
    /// 参数说明：无。
    ///
    /// 返回：已通过命名空间标记校验的协议配置，不根据收到的业务消息切换协议。
    pub fn profile(&self) -> CompatibilityProfile {
        self.cfg.profile
    }

    /// 业务作用：返回语言无关的逻辑数据源标识；持久键、任务身份和指标维度均使用该值。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：启动期已规范化并冻结的 source id，默认值为 `primary`。
    pub fn qualifier(&self) -> &str {
        &self.cfg.qualifier
    }

    /// 业务作用：返回当前客户端的冻结配置。
    ///
    /// 参数说明：无。
    ///
    /// 返回：创建客户端时使用的配置借用，不开放修改；其中可能包含连接凭据，不应直接对外输出。
    pub fn config(&self) -> &RedisConfig {
        &self.cfg
    }

    /// 业务作用：取得普通命令使用的共享传输句柄，避免每条命令独立建立连接。
    ///
    /// 参数说明：无。
    ///
    /// 返回：当前单节点或集群传输的克隆句柄；共享底层连接，不提供阻塞命令隔离或本层命令超时。
    pub fn conn(&self) -> Conn {
        self.conn.clone()
    }

    /// 业务作用：按配置为管线提供独立传输，降低大批次对普通命令和控制操作的排队影响。
    ///
    /// 参数说明：无。
    ///
    /// 返回：启用 `pipeline.dedicated_conn` 时惰性建立并共享专用句柄，失败返回连接错误且不保存失败值。
    /// 关闭时直接返回普通命令的共享句柄，不提供管线流量隔离。
    pub(crate) async fn pipe_conn(&self) -> Result<Conn> {
        if !self.cfg.pipeline.dedicated_conn {
            return Ok(self.conn());
        }
        let c = self
            .pipe_cell
            .get_or_try_init(|| self.build_transport("pipeline 专用"))
            .await?;
        Ok(c.clone())
    }

    /// 业务作用：为单个 Job source 建立启用 RESP3 Push 的独占 Pub/Sub lane，通知断线与重订不影响控制命令。
    ///
    /// 参数说明：
    /// - `push_sender`：由调用方持有消费责任的 RESP3 推送接收入口，用于承接订阅事件。
    ///
    /// 返回：绑定推送入口的独立连接，订阅命令由调用方继续发送；握手或拓扑建立失败返回错误。
    #[cfg(feature = "job")]
    pub(crate) async fn job_pubsub_conn(
        &self,
        push_sender: Arc<dyn redis::aio::AsyncPushSender>,
    ) -> Result<Conn> {
        let timeout_ms = self.cfg.command.response_timeout_ms;
        let conn = if self.is_cluster {
            let mut builder = redis::cluster::ClusterClient::builder(vec![self.cfg.url.as_str()])
                .use_protocol(redis::ProtocolVersion::RESP3)
                .push_sender(push_sender.clone());
            if timeout_ms > 0 {
                let timeout = Duration::from_millis(timeout_ms);
                builder = builder
                    .response_timeout(timeout)
                    .connection_timeout(timeout);
            }
            let client = builder.build().map_err(NasaRedisError::Redis)?;
            Conn::Cluster(
                await_with_deadline(
                    client.get_async_connection(),
                    timeout_ms,
                    "Job RESP3 Cluster lane 建连",
                )
                .await
                .map_err(NasaRedisError::Redis)?,
            )
        } else {
            let mut info = self
                .cfg
                .url
                .as_str()
                .into_connection_info()
                .map_err(NasaRedisError::Redis)?;
            let redis_settings = info
                .redis_settings()
                .clone()
                .set_protocol(redis::ProtocolVersion::RESP3);
            info = info.set_redis_settings(redis_settings);
            let client = redis::Client::open(info).map_err(NasaRedisError::Redis)?;
            let mut manager = redis::aio::ConnectionManagerConfig::new()
                .set_push_sender(push_sender)
                .set_automatic_resubscription();
            if timeout_ms > 0 {
                let timeout = Duration::from_millis(timeout_ms);
                manager = manager
                    .set_response_timeout(Some(timeout))
                    .set_connection_timeout(Some(timeout));
            }
            Conn::Single(
                await_with_deadline(
                    ConnectionManager::new_with_config(client, manager),
                    timeout_ms,
                    "Job RESP3 lane 建连",
                )
                .await
                .map_err(NasaRedisError::Redis)?,
            )
        };
        Ok(conn)
    }

    /// 业务作用：返回 Job 状态脚本独占的惰性连接，隔离租约、领取和完成提交与业务命令流量。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：首次调用建立独立传输，后续复用其可克隆句柄；建连失败保留底层错误，不缓存失败值。
    #[cfg(feature = "job")]
    pub(crate) async fn job_control_conn(&self) -> Result<Conn> {
        let conn = self
            .job_control_cell
            .get_or_try_init(|| self.build_transport("Job control lane"))
            .await?;
        Ok(conn.clone())
    }

    /// 业务作用：返回 Job 心跳独占的惰性连接，使截止内重发不影响其它状态动作的结局分类。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：首次心跳建立独立 transport，后续复用可自动重连的句柄；建连失败返回传输错误。
    #[cfg(feature = "job")]
    pub(crate) async fn job_heartbeat_conn(&self) -> Result<Conn> {
        let conn = self
            .job_heartbeat_cell
            .get_or_try_init(|| self.build_transport("Job heartbeat lane"))
            .await?;
        Ok(conn.clone())
    }

    /// 业务作用：按冻结拓扑新建独立传输，隔离管线、Job 控制或阻塞读取的连接排队。
    ///
    /// 参数说明：
    /// - `what`: 错误或超时日志中标识当前操作的名称。
    ///
    /// 返回：按已探测拓扑建立独立单节点或 Cluster transport；建连或截止时间失败返回带操作归因的传输错误。
    async fn build_transport(&self, what: &str) -> Result<Conn> {
        let resp_timeout = self.cfg.command.response_timeout_ms;
        if self.is_cluster {
            let mut builder = redis::cluster::ClusterClient::builder(vec![self.cfg.url.as_str()]);
            if resp_timeout > 0 {
                let d = Duration::from_millis(resp_timeout);
                builder = builder.response_timeout(d).connection_timeout(d);
            }
            let cc = builder.build()?;
            let conn = await_with_deadline(
                cc.get_async_connection(),
                resp_timeout,
                &format!("{what} cluster 建连"),
            )
            .await?;
            Ok(Conn::Cluster(conn))
        } else {
            let mut mgr_cfg = redis::aio::ConnectionManagerConfig::new();
            if resp_timeout > 0 {
                let d = Duration::from_millis(resp_timeout);
                mgr_cfg = mgr_cfg
                    .set_response_timeout(Some(d))
                    .set_connection_timeout(Some(d));
            }
            let conn = await_with_deadline(
                ConnectionManager::new_with_config(self.raw.clone(), mgr_cfg),
                resp_timeout,
                &format!("{what} 建连"),
            )
            .await?;
            Ok(Conn::Single(conn))
        }
    }

    /// 业务作用：向需要自行派生专用连接的内部组件提供底层客户端配置入口。
    ///
    /// 参数说明：无。
    ///
    /// 返回：种子端点客户端的借用，不立即建连；调用方负责新连接的超时、拓扑和生命周期。
    pub(crate) fn raw_client(&self) -> &redis::Client {
        &self.raw
    }

    /// 业务作用：为阻塞读取派生独立传输，防止 `XREAD` 或 `XREADGROUP BLOCK` 阻塞普通命令的响应。
    ///
    /// 参数说明：
    /// - `what`：连接角色的固定名称，用于失败归因，不携带业务载荷或凭据。
    ///
    /// 返回：由调用方独立持有的单节点或集群连接；建连失败返回错误。
    /// 连接级响应超时仍有效，调用方须确保正常阻塞时长低于该上限。
    pub(crate) async fn dedicated_conn(&self, what: &str) -> Result<Conn> {
        self.build_transport(what).await
    }

    /// 业务作用：把客户端状态脚本事件绑定到当前 source generation 的固定指标容器。
    ///
    /// 参数说明：
    /// - `metrics`：当前运行代际的指标容器，必须与本客户端的规范化数据源身份一致。
    ///
    /// 返回：无；新 generation 原子替换旧弱引用，客户端不拥有运行时生命周期。
    #[cfg(feature = "job")]
    pub(crate) fn attach_job_metrics(&self, metrics: &Arc<crate::job::metrics::JobMetrics>) {
        *self
            .job_metrics
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::downgrade(metrics));
    }

    /// 业务作用：记录控制面因目标 Redis 未缓存脚本而执行整文本回退，供 source 观测脚本缓存换代。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：无；运行时尚未绑定或已经结束时不建立游离指标。
    #[cfg(feature = "job")]
    pub(crate) fn record_job_script_reload(&self) {
        let metrics = self
            .job_metrics
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(std::sync::Weak::upgrade);
        if let Some(metrics) = metrics {
            metrics.add("redis_job_script_reload_total", 1);
        }
    }

    /// 业务作用：执行高层封装未覆盖的原始 Redis 命令，并沿用客户端的单命令时间预算。
    ///
    /// 参数说明：
    /// - `cmd`：调用方构造的命令及参数，调用方负责授权、键布局与命令副作用。
    ///
    /// 返回：按目标类型解码的结果；预算、传输、服务器或类型转换失败返回错误。
    /// 超时或断链不证明远端未执行，非幂等命令不得据此无条件重试。
    pub async fn execute_raw<T: redis::FromRedisValue>(&self, cmd: &redis::Cmd) -> Result<T> {
        let mut c = self.conn();
        self.timed(cmd.query_async(&mut c)).await
    }

    /// 业务作用：低频健康探针(napp readiness 复用):执行 `PING` 判活。
    ///
    /// 成功表示连接可用且服务器响应;沿用 `command.timeout_ms` 单命令超时。只做只读判活,
    /// 不改变任何拓扑或数据。上层据此发布运行期就绪观测。
    ///
    /// 参数说明：无。
    ///
    /// 返回：命令取得有效 Redis 响应时返回 `Ok(())`；连接、超时或协议错误返回 `Err`。
    /// 不检查响应文本是否为 `PONG`，也不证明业务脚本或所需 ACL 全部可用。
    pub async fn ping(&self) -> Result<()> {
        self.execute_raw::<redis::Value>(&redis::cmd("PING"))
            .await
            .map(|_| ())
    }

    /// 业务作用：记录客户端的显式停机调用，供外层生命周期协调器关联停机阶段。
    ///
    /// 参数说明：无。
    ///
    /// 返回：记录日志后完成；不关闭仍被持有的连接，不取消消费或看门狗，也不拒绝后续命令。
    /// 调用方须另行停止组件任务并释放所有传输句柄。
    pub async fn shutdown(&self) {
        tracing::info!(namespace = %self.cfg.namespace, "RedisClient shutdown");
    }
}

/// 显式注册表(重复 qualifier 拒绝、注销;主要注入方式仍是显式传 Arc)。
#[derive(Default)]
pub struct RedisRegistry {
    /// 按冻结数据源身份持有的客户端表；读写锁保证注册与注销的单次原子性。
    map: RwLock<HashMap<String, Arc<RedisClient>>>,
}

impl RedisRegistry {
    /// 业务作用：创建空的命名客户端注册表。
    ///
    /// 参数说明：无。
    ///
    /// 返回：不含任何客户端的注册表，不建立连接或启动后台任务。
    pub fn new() -> Self {
        Self::default()
    }

    /// 业务作用：以与客户端一致的数据源身份注册共享句柄，拒绝同名覆盖以免切换业务连接。
    ///
    /// 参数说明：
    /// - `qualifier`: 必须与客户端冻结身份一致的注册名称。
    /// - `client`: 要注册的 Redis 客户端共享句柄。
    ///
    /// 返回：首次同名注册成功时保存句柄；身份不一致或名称已存在返回配置错误并保留原表项。
    pub fn register(&self, qualifier: &str, client: Arc<RedisClient>) -> Result<()> {
        // 名称用于选择数据源；拒绝别名冒充，防止按一个身份取得另一个命名空间的连接。
        if qualifier != client.qualifier() {
            return Err(NasaRedisError::Config(format!(
                "注册表 key {qualifier} 与 RedisClient qualifier {} 不一致",
                client.qualifier()
            )));
        }
        let mut m = self.map.write().unwrap_or_else(|e| e.into_inner());
        // 已发布的同名句柄可能仍被业务持有，覆盖会产生同一身份指向不同连接的并行状态。
        if m.contains_key(qualifier) {
            return Err(NasaRedisError::Config(format!(
                "qualifier 重复注册: {qualifier}"
            )));
        }
        m.insert(qualifier.to_string(), client);
        Ok(())
    }

    /// 业务作用：返回指定 qualifier 对应的 Redis 客户端。
    ///
    /// 参数说明：
    /// - `qualifier`: 注册时使用的唯一业务名。
    ///
    /// 返回：存在时克隆共享句柄，调用方可在注册表注销后继续持有；不存在时返回 `None`。
    pub fn get(&self, qualifier: &str) -> Option<Arc<RedisClient>> {
        self.map
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(qualifier)
            .cloned()
    }

    /// 业务作用：注销指定 qualifier 对应的 Redis 客户端。
    ///
    /// 参数说明：
    /// - `qualifier`: 要从注册表移除的唯一业务名。
    ///
    /// 返回：移除并交还已有句柄，不存在时返回 `None`；不会取消其它持有者的连接或任务。
    pub fn deregister(&self, qualifier: &str) -> Option<Arc<RedisClient>> {
        self.map
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(qualifier)
    }
}
