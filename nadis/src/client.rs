// ============================================================================
// src/client.rs —— RedisClient:连接、协议标记、注册表、停机(文档)。
//
// 装配模型:`RedisClient::connect(cfg)` 只建连接 + 协议标记校验,
// **不无条件启动任何高级设施后台任务**;锁/partition/search 各自显式构造。
// 协议标记:nasa:protocol:{namespace},SET NX 创建或校验,
// profile / naming_config_hash 不一致即拒绝启动(fail-closed)。
// 注:标记仅 Rust 节点写入/校验——原实现 端不感知,防的是"Rust 误配 profile",
// 防不了"原实现 节点误连",后者依赖部署侧保证环境隔离。
// 多数据源 = 多个配置多次 connect;推荐显式传 Arc,RedisRegistry 仅迁移期便利通道。
// ============================================================================

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use redis::aio::ConnectionManager;
use redis::AsyncCommands;

use crate::config::{CompatibilityProfile, RedisConfig};
use crate::error::{NasaRedisError, Result};

/// 传输抽象(④A Cluster transport):单节点 `ConnectionManager` 或 `cluster_async::ClusterConnection`。
/// 两者均 impl `ConnectionLike`(故 `AsyncCommands`/`query_async` 在 `Conn` 上通用),Clone 廉价(多路
/// 复用句柄)。Cluster 变体**跟随 MOVED/ASK**,单键命令按 slot 路由到正确节点;多键 Lua/MULTI 仍要求
/// 同槽 key(否则 CROSSSLOT)——partition 多键脚本的同槽保证见 ④B relayout。
#[derive(Clone)]
pub enum Conn {
    /// 单点 Redis 连接管理器。
    Single(ConnectionManager),
    /// Redis Cluster 异步连接,可跟随 MOVED/ASK 重定向。
    Cluster(redis::cluster_async::ClusterConnection),
}

impl redis::aio::ConnectionLike for Conn {
    // Routes one packed command through the selected connection mode.
    ///
    /// # 参数
    /// - `cmd`: 底层 Redis 命令对象。
    fn req_packed_command<'a>(
        &'a mut self,
        cmd: &'a redis::Cmd,
    ) -> redis::RedisFuture<'a, redis::Value> {
        match self {
            Conn::Single(c) => c.req_packed_command(cmd),
            Conn::Cluster(c) => c.req_packed_command(cmd),
        }
    }

    // Routes a packed command batch through the selected connection mode.
    ///
    /// # 参数
    /// - `cmd`: 底层 Redis 命令对象。
    /// - `offset`: 列表、字符串或分页操作的偏移量。
    /// - `count`: Redis 命令、分页或批处理使用的数量上限。
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

    /// 返回当前 Redis 连接配置的逻辑库编号。
    ///
    /// 单机连接返回实际 `SELECT` 的 db；cluster 连接返回配置值,用于上层保持统一的客户端配置视图。
    fn get_db(&self) -> i64 {
        match self {
            Conn::Single(c) => c.get_db(),
            Conn::Cluster(c) => c.get_db(),
        }
    }
}

/// 协议标记 key。
///
/// # 参数
/// - `namespace`: Redis key 使用的业务命名空间。
fn marker_key(namespace: &str) -> String {
    format!("nasa:protocol:{namespace}")
}

/// canonical 命名配置串:字段顺序固定、缺省值展开。
///**存完整 canonical 串做相等比对**,不再用 CRC16——16 位摘要碰撞域仅 65536,
/// 不同命名配置可能撞同摘要使 fail-closed 判重 **fail-open**(误配被当幂等放行)。CRC16 是校验位非
/// 抗碰撞摘要;完整串相等比对零碰撞,成本仅几十字节。
///
/// # 参数
/// - `cfg`: 配置对象,用于初始化组件或校验运行参数。
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
    // **空 groups 逐字节保持旧 v1 串**(不破坏现存单组/legacy CRC16 部署);非空才追加稳定序列化 groups。
    if cfg.partition.groups.is_empty() {
        base
    } else {
        format!("{base}|partition.groups={}", canonical_groups(cfg))
    }
}

/// 稳定序列化多组配置(label `partition.groups`)。**结构化 JSON**(非手拼分隔符,含 `,`/`;`
/// 的 topic 会注入碰撞 fail-open;JSON 转义单射)。形态 `{default:<runtime>, groups:{逻辑名:<topics+runtime>}}`:
/// - **`default`**:默认组 resolved runtime(= 父级 PartitionCfg + 全局 StreamCfg;默认组也消费
///   所有未隔离 topic,其 rebalance/min_idle/drain/batch 等不一致同样致跨节点 liveness/背压/drain 判定分裂,
///   故必须进 marker——此前只序列化隔离组 = 默认组运行参数 fail-open)。
/// - **`groups`**:各隔离组 resolved(topics 排序 + 空 topics 归一逻辑名 + count/runtime 取 override→父级/全局)。
///
/// 所有字段 resolved(`0 继承`与显式父级值同 canonical,不误 fail-closed);BTreeMap 排序键稳定。
///
/// # 参数
/// - `cfg`: 配置对象,用于初始化组件或校验运行参数。
fn canonical_groups(cfg: &RedisConfig) -> String {
    let p = &cfg.partition;
    let s = &cfg.stream;
    /// 保存 CanonRuntime 运行状态；用于管理后台任务和共享资源。
    #[derive(serde::Serialize)]
    struct CanonRuntime {
        count: u32,
        rebalance_ms: u64,
        min_idle_ms: u64,
        holds_check_interval_ms: u64,
        drain_timeout_ms: u64,
        handler_timeout_ms: u64,
        max_redeliver: u32,
        poison_policy: crate::config::PoisonPolicy,
        batch_size: usize,
        poll_timeout_ms: u64,
        inflight_max: usize,
    }
    /// 承载规范化后的分组名；用于统一 Redis 分组标记的比较口径。
    #[derive(serde::Serialize)]
    struct CanonGroup<'a> {
        topics: Vec<&'a str>,
        #[serde(flatten)]
        rt: CanonRuntime,
    }
    /// 承载规范化后的分片名；用于统一 Redis 分片标记的比较口径。
    #[derive(serde::Serialize)]
    struct CanonPartition<'a> {
        default: CanonRuntime,
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
                    count: if g.count > 0 { g.count } else { p.count },
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
    serde_json::to_string(&CanonPartition { default, groups })
        .expect("canonical partition json 序列化不应失败")
}

/// 兼容**旧格式 marker**( 升级前的 `{"profile":..,"naming_config_hash":"<crc16>"}`):
/// 升级到完整 canonical 串后,既有部署里残留的旧 marker **不应因格式变更被 fail-closed 拒启动**。
/// 旧 marker 仍按其 CRC16 语义校验(profile 相等 + hash==crc16(本节点 canonical));通过即放行(不重写)。
/// 新建一律写完整串(零碰撞);旧 marker 自然老化(删/重建后变新格式)。返回 true=旧 marker 校验通过。
///
/// # 参数
/// - `existing`: Redis 中已存在的协议标记或缓存值。
/// - `cfg`: 配置对象,用于初始化组件或校验运行参数。
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

/// 协议标记**语义相等**:把两边 parse 成 `serde_json::Value` 比较(键值集合相等,
/// 与键序无关)。先前依赖 serde_json 默认字母序做裸字符串相等,开启 preserve_order
/// 后键序变为声明序,旧部署里残留的字母序 marker 会被误判不一致——故改成语义比对。
/// parse 失败(非 JSON)退回原始字符串相等(保守)。
/// 构造协议标记 payload:`{"profile": <wire_name>, "naming_config": <canonical>}`。
/// profile 用**持久 wire 名**(`wire_name()`),而非枚举 Debug 名,保升级兼容。
///
/// # 参数
/// - `cfg`: 配置对象,用于初始化组件或校验运行参数。
fn marker_payload(cfg: &RedisConfig) -> String {
    serde_json::json!({
        "profile": cfg.profile.wire_name(),
        "naming_config": naming_config_canonical(cfg),
    })
    .to_string()
}

/// 比较协议标记的语义等价性；用于兼容旧格式和新格式的启动校验。
///
/// # 参数
/// - `existing`: Redis 中已存在的协议标记或缓存值。
/// - `payload`: 当前配置生成的协议标记 JSON 字符串。
fn marker_semantically_equal(existing: &str, payload: &str) -> bool {
    match (
        serde_json::from_str::<serde_json::Value>(existing),
        serde_json::from_str::<serde_json::Value>(payload),
    ) {
        (Ok(a), Ok(b)) => a == b,
        _ => existing == payload,
    }
}

/// Redis 客户端(对照 原实现 RedisProxy 的连接/注册表职责;命令 API 见 commands.rs)。
pub struct RedisClient {
    cfg: RedisConfig,
    /// 连接管理器:clone 共享同一 socket,**断线后台自动重连**(direct command 角色,
    /// 连接角色表;裸 MultiplexedConnection 断线后不恢复,改用
    /// ConnectionManager)。**注意**:重连是后台进行的,**命中断开的
    /// 那一条命令仍会返回 IO 错误**(不是透明重试成功)——框架不能透明重试非幂等命令;
    /// 后续命令在重连完成后恢复。调用方必须自行处理首次 IO 错误 / ExecutionUnknown。
    conn: Conn,
    /// 底层 client:派生专用连接(Pub/Sub、阻塞命令)用(cluster 下为 seed 节点;classic pubsub 跨节点)。
    raw: redis::Client,
    /// 目标是否 Cluster 模式(④A:transport 为 ClusterConnection;partition 多键脚本需 ④B relayout)。
    is_cluster: bool,
    /// **pipeline 专用 lane**:独立多路复用连接,与 direct/control/lock 隔离,
    /// 防大批量 pipeline 头阻塞共享连接。**惰性**——首次 pipeline 才建连(`pipeline.dedicated_conn=true` 时)。
    pipe_cell: tokio::sync::OnceCell<Conn>,
}

/// 给建连 future 套整体 deadline(`ms=0` 不限)。超时返回 IoError(Redis 不可达
/// 时 `connect()` 不再永久 pending,启动 fail-fast 生效)。
///
/// # 参数
/// - `fut`: 需要在 Redis 执行上下文中运行的异步任务。
/// - `ms`: 毫秒时间值。
/// - `what`: 错误或超时日志中标识当前操作的名称。
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

/// 把某建连阶段的底层 redis 错误包装成带阶段与脱敏 endpoint 的 [`NasaRedisError::ConnectProbe`]。
///
/// # 参数
/// - `stage`: 失败阶段词(`connect`/`topology`/`cluster-discovery`/`protocol-marker`)。
/// - `cfg`: 当前 Redis 配置,取其 `safe_endpoint()` 作定位信息(不含凭据)。
/// - `source`: 底层 redis 错误,原样保留为错误链根因。
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
    /// 构造即就绪:建连接 → 校验/创建协议标记 → 返回 Arc。
    ///
    /// 各建连阶段的失败都经内部 `connect_probe` 标注阶段与脱敏 endpoint 并保留底层错误链,让启动
    /// 诊断能一次性定位是哪一步、什么根因;单连接建连/拓扑探测/标记读写的所有权语义保持不变。
    ///
    /// # 参数
    /// - `cfg`: Redis 连接、命名空间、profile 和各子模块运行参数。
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
        // ④A:检测到 Cluster → 用 `cluster_async::ClusterConnection`(跟随 MOVED/ASK)。
        //**INFO 失败 = 连接故障,不是 standalone 信号**——实测健康 standalone 的
        // `INFO cluster` 成功返回 `cluster_enabled:0`(不报错),故 INFO 报错/超时只能是 unreachable/超时/
        // ACL 禁 INFO。此前"INFO 失败→静默当 standalone"会把连接故障误判成拓扑决策(目标实为 cluster 时
        // 建出不跟 MOVED 的 Single → 运行期 MOVED 风暴)。改:**重试 3 次容忍瞬时抖动,仍失败则 fail-closed 拒启**。
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
                        detected = Some(info.contains("cluster_enabled:1"));
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
            // ④A Cluster transport:seed URL 触发拓扑发现,跟随 MOVED/ASK 路由。
            // P1-B:同样设连接级 response_timeout(覆盖 cluster 下裸命令)。
            let mut builder = redis::cluster::ClusterClient::builder(vec![cfg.url.as_str()]);
            if resp_timeout > 0 {
                let d = Duration::from_millis(resp_timeout);
                builder = builder.response_timeout(d).connection_timeout(d);
            }
            let cc = builder
                .build()
                .map_err(|e| connect_probe("cluster-discovery", &cfg, e))?;
            tracing::info!(namespace = %cfg.namespace, "目标 Redis 为 Cluster 模式,启用 cluster_async transport(跟随 MOVED/ASK)");
            // P1:cluster 建连也套整体 deadline(get_async_connection 触发拓扑发现,可能久挂)
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
        }))
    }

    /// 目标是否 Cluster 模式(④A)。partition 多键脚本在 cluster 下需 ④B relayout 保证同槽。
    pub fn is_cluster(&self) -> bool {
        self.is_cluster
    }

    /// Returns the active compatibility profile.
    pub fn profile(&self) -> CompatibilityProfile {
        self.cfg.profile
    }

    /// Returns the client configuration.
    pub fn config(&self) -> &RedisConfig {
        &self.cfg
    }

    /// direct command 连接(`Conn`:单节点 ConnectionManager 或 cluster ClusterConnection,
    /// clone 共享多路复用句柄 + 自动重连/重路由; 角色表第一行)。
    pub fn conn(&self) -> Conn {
        self.conn.clone()
    }

    /// **pipeline 专用连接**:`pipeline.dedicated_conn=true`(默认)时返回一条
    /// 与 direct/control/lock 隔离的独立多路复用连接(**惰性创建**,首次调用建连、之后 clone 复用);
    /// `=false` 则退回共享 `conn()`(旧行为)。pipeline 路径(`PipelineSession`/`AutoPipeline`)走此。
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

    /// 新建一条独立 transport(与 connect() 的主连接同款建法;供 pipeline lane 惰性建连)。
    ///
    /// # 参数
    /// - `what`: 错误或超时日志中标识当前操作的名称。
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

    /// 派生专用连接(Pub/Sub / 阻塞命令角色;调用方持有生命周期)。
    pub(crate) fn raw_client(&self) -> &redis::Client {
        &self.raw
    }

    /// 派生一条**独立 transport**(与主连接同款建法),供 stream 阻塞订阅等【阻塞命令】专用。
    /// 关键:XREAD/XREADGROUP `BLOCK` 若走共享 `conn()` 会把并发命令排在阻塞响应之后(实测吞吐退化到 ~1/s),
    /// 故订阅任务持有自己的一条连接。cluster 下是独立 `ClusterConnection`(按 stream key slot 路由,非 seed 单连接)。
    /// 连接级 `response_timeout_ms`(默认 30s)照常生效——远大于典型 `block_ms`(500ms),不会误杀 `BLOCK`,
    /// 但网络真断时能让阻塞读返错、订阅任务据此重建连接。
    pub(crate) async fn dedicated_conn(&self, what: &str) -> Result<Conn> {
        self.build_transport(what).await
    }

    /// 受控原始入口:不暴露 ConnectionManager 等底层类型,
    /// 调用方组装 redis::Cmd,本方法执行——逃生舱,非常规路径。
    /// M2:同样套 `command.timeout_ms`(单命令逃生舱也受单命令超时约束)。
    ///
    /// # 参数
    /// - `cmd`: 底层 Redis 命令对象。
    pub async fn execute_raw<T: redis::FromRedisValue>(&self, cmd: &redis::Cmd) -> Result<T> {
        let mut c = self.conn();
        self.timed(cmd.query_async(&mut c)).await
    }

    /// 低频健康探针(napp readiness 复用):执行 `PING` 判活。
    ///
    /// 成功表示连接可用且服务器响应;沿用 `command.timeout_ms` 单命令超时。只做只读判活,
    /// 不改变任何拓扑或数据。上层据此发布运行期就绪观测。
    ///
    /// # 返回
    ///
    /// 服务器正常应答返回 `Ok(())`;连接、超时或协议错误返回 `Err`。
    pub async fn ping(&self) -> Result<()> {
        self.execute_raw::<redis::Value>(&redis::cmd("PING"))
            .await
            .map(|_| ())
    }

    /// 显式停机:multiplexed 连接随 Arc 释放;此处为对称占位,
    /// 高级设施(锁看门狗等)各自持 CancellationToken 自行退出。
    pub async fn shutdown(&self) {
        tracing::info!(namespace = %self.cfg.namespace, "RedisClient shutdown");
    }
}

/// 显式注册表(重复 qualifier 拒绝、注销;主要注入方式仍是显式传 Arc)。
#[derive(Default)]
pub struct RedisRegistry {
    map: RwLock<HashMap<String, Arc<RedisClient>>>,
}

impl RedisRegistry {
    /// Creates a new instance.
    pub fn new() -> Self {
        Self::default()
    }

    /// 注册;重复 qualifier 返回错误(不静默覆盖)。
    ///锁中毒**恢复**而非 panic——表内只存 `Arc` clone,panic 不会留下不一致状态,
    /// `into_inner()` 拿回守卫继续用,比让整个注册表 API 在一次无关 panic 后全 panic 稳健。
    ///
    /// # 参数
    /// - `qualifier`: 客户端在注册表中的唯一业务名。
    /// - `client`: 要注册的 Redis 客户端共享句柄。
    pub fn register(&self, qualifier: &str, client: Arc<RedisClient>) -> Result<()> {
        let mut m = self.map.write().unwrap_or_else(|e| e.into_inner());
        if m.contains_key(qualifier) {
            return Err(NasaRedisError::Config(format!(
                "qualifier 重复注册: {qualifier}"
            )));
        }
        m.insert(qualifier.to_string(), client);
        Ok(())
    }

    /// 返回指定 qualifier 对应的 Redis 客户端。
    ///
    /// # 参数
    /// - `qualifier`: 注册时使用的唯一业务名。
    pub fn get(&self, qualifier: &str) -> Option<Arc<RedisClient>> {
        self.map
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(qualifier)
            .cloned()
    }

    /// 注销指定 qualifier 对应的 Redis 客户端。
    ///
    /// # 参数
    /// - `qualifier`: 要从注册表移除的唯一业务名。
    pub fn deregister(&self, qualifier: &str) -> Option<Arc<RedisClient>> {
        self.map
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(qualifier)
    }
}
