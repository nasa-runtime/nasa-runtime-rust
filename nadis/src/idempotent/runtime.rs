//! 幂等计数运行时：一次 Lua 完成判重、计数与凭证登记；布局在首次使用时惰性解析并冻结。
//!
//! 基础命令本身不触发账本准备；运行时经 `OnceCell` 惰性初始化，显式准备、幂等直发或
//! 幂等 Pipeline 入口负责解析共享布局和探测能力。

use std::collections::BTreeSet;
use std::sync::Arc;

use redis::cluster_routing::{MultipleNodeRoutingInfo, RoutingInfo, SingleNodeRoutingInfo};
use redis::{ErrorKind, ServerErrorKind, Value};
use tokio::sync::OnceCell;

use crate::client::{Conn, RedisClient};
use crate::error::{NasaRedisError, Result};
use crate::keytag::redis_slot;

use super::config::{IdempotentLayoutSnapshot, IdempotentTtlMode};
use super::metrics::IdempotentCounterMetrics;
use super::result::{IdempotentCounterError, IdempotentRejection, IdempotentResultCode};
use super::wire;

/// HASH_FIELD 布局脚本文本；随 crate 归档，编译期确认存在。
const HFE_SCRIPT: &str = include_str!("lua/idempotent_counter_hfe.lua");
/// HASH_BUCKET 布局脚本文本；随 crate 归档，编译期确认存在。
const BUCKET_SCRIPT: &str = include_str!("lua/idempotent_counter_bucket.lua");

/// 一次幂等计数脚本调用的二进制键、参数、脚本文本与缓存摘要。
type IdempotentInvocation<'a> = (Vec<Vec<u8>>, Vec<Vec<u8>>, &'static str, &'a str);

/// 首次请求实际执行的计数操作；`family` 不含方向，使同一 nonce 在增减漂移时仍命中首次结果。
#[derive(Debug, Clone, Copy)]
pub(crate) enum Operation {
    /// 字符串整数计数按给定步长增加。
    StringIncrement,
    /// 字符串整数计数按给定步长减少。
    StringDecrement,
    /// Hash 整数字段按给定步长增加。
    HashIncrement,
    /// Hash 整数字段按给定步长减少。
    HashDecrement,
    /// 有序集合成员按给定浮点步长增加分数。
    ZSetIncrement,
}

impl Operation {
    /// 业务作用：返回不含方向的结构身份字节，进入 record field 与 ledger shard 摘要。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：字符串、Hash 或有序集合的固定类别字节，增减方向共用同一类别。
    fn family(self) -> u8 {
        match self {
            Self::StringIncrement | Self::StringDecrement => wire::FAMILY_STRING,
            Self::HashIncrement | Self::HashDecrement => wire::FAMILY_HASH,
            Self::ZSetIncrement => wire::FAMILY_ZSET,
        }
    }

    /// 业务作用：返回脚本 `ARGV[2]` 的操作分支标识；文本是脚本协议的一部分，不可改动。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：`STR_INCR`/`STR_DECR`/`HASH_INCR`/`HASH_DECR`/`ZSET_INCR` 的 ASCII 字节。
    fn command(self) -> &'static [u8] {
        match self {
            Self::StringIncrement => b"STR_INCR",
            Self::StringDecrement => b"STR_DECR",
            Self::HashIncrement => b"HASH_INCR",
            Self::HashDecrement => b"HASH_DECR",
            Self::ZSetIncrement => b"ZSET_INCR",
        }
    }
}

/// 幂等计数运行时；持有已校验布局、指标与惰性解析的运行模式。
pub(crate) struct IdempotentCounterRuntime {
    /// 客户端构造时验证并冻结的本地账本配置。
    layout: IdempotentLayoutSnapshot,
    /// 布局选择、结算和回收异常的本地观测事实。
    metrics: Arc<IdempotentCounterMetrics>,
    /// 与共享标记确认一致后缓存的过期模式；准备失败不写入。
    resolved: OnceCell<IdempotentTtlMode>,
    /// 字段过期布局的脚本缓存摘要，用于优先尝试 EVALSHA。
    hfe_sha: String,
    /// 环形桶布局的脚本缓存摘要，用于优先尝试 EVALSHA。
    bucket_sha: String,
}

/// Cluster 全量响应的能力结论；“不支持”与“无法完整证明”必须分开，后者不能永久建成降级 marker。
enum ClusterProbeCoverage {
    /// 拓扑与响应覆盖一致，全部节点确认支持所需命令。
    Supported,
    /// 节点响应覆盖完整，至少一个节点未报告支持命令。
    Unsupported,
    /// 拓扑或响应集合不能形成完整覆盖证据。
    Incomplete,
}

impl IdempotentCounterRuntime {
    /// 业务作用：从已校验配置快照创建运行时，并预计算脚本 SHA1；不触发任何 Redis 往返。
    ///
    /// 参数说明：
    /// - `layout`: 已通过 `snapshot()` 校验的账本布局。
    ///
    /// 返回：尚未解析运行模式、尚未探测能力的运行时。
    pub(crate) fn new(layout: IdempotentLayoutSnapshot) -> Self {
        Self {
            layout,
            metrics: Arc::new(IdempotentCounterMetrics::default()),
            resolved: OnceCell::new(),
            hfe_sha: redis::Script::new(HFE_SCRIPT).get_hash().to_owned(),
            bucket_sha: redis::Script::new(BUCKET_SCRIPT).get_hash().to_owned(),
        }
    }

    /// 业务作用：返回长期复用的观测容器，供健康端点与指标桥接读取。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：与运行时同生命周期的指标句柄。
    pub(crate) fn metrics(&self) -> &Arc<IdempotentCounterMetrics> {
        &self.metrics
    }

    /// 业务作用：执行整数幂等计数并保持 int64 精度，协议值异常时拒绝静默折叠为零。
    ///
    /// 参数说明：
    /// - `client`: 承载命令连接的 RedisClient。
    /// - `op`：整数计数的结构类别和增减方向。
    /// - `key`：目标计数键，决定计数与账本共同使用的槽位。
    /// - `member`：Hash 字段原始字节；字符串计数使用空字节。
    /// - `delta`：待编码为十进制文本的有符号步长，按操作方向解释。
    /// - `nonce`：稳定业务事件标识，窗口内重复请求复用首次结果。
    ///
    /// 返回：首次请求执行后的精确 int64；重复 nonce 返回首次值；拒绝或非 int64 返回错误。
    pub(crate) async fn execute_long(
        &self,
        client: &RedisClient,
        op: Operation,
        key: &str,
        member: &[u8],
        delta: i64,
        nonce: &str,
    ) -> Result<i64> {
        let value = self
            .execute(
                client,
                op,
                key,
                member,
                delta.to_string().into_bytes(),
                nonce,
            )
            .await?;
        value
            .parse::<i64>()
            .map_err(|_| protocol("idempotent integer script returned a non-int64 value"))
    }

    /// 业务作用：执行 ZSET 幂等计数，保留 Redis 浮点语义（含 ±inf）。
    ///
    /// 参数说明：
    /// - `client`：承载账本准备和脚本调用的 Redis 客户端。
    /// - `key`：目标有序集合键，决定账本槽位。
    /// - `member`：成员的原始命令字节，与判重摘要使用同一编码。
    /// - `delta`：首次结算使用的浮点分数增量。
    /// - `nonce`：稳定业务事件标识，窗口内重复请求复用首次结果。
    ///
    /// 返回：首次请求执行后的精确 double；重复 nonce 返回首次值；拒绝或非数值返回错误。
    pub(crate) async fn execute_double(
        &self,
        client: &RedisClient,
        key: &str,
        member: &[u8],
        delta: f64,
        nonce: &str,
    ) -> Result<f64> {
        // ZINCRBY 增量以文本发送；Rust Display 对 ±inf 给出 Redis 接受的 inf/-inf，NaN 交由原生命令拒绝。
        let delta_text = format!("{delta}").into_bytes();
        let value = self
            .execute(
                client,
                Operation::ZSetIncrement,
                key,
                member,
                delta_text,
                nonce,
            )
            .await?;
        parse_redis_double(&value)
    }

    /// 业务作用：派生同 slot 账本键并执行原子判重协议；成功返回首次值文本，拒绝返回结构化错误。
    ///
    /// 参数说明：
    /// - `client`：承载共享布局准备和脚本执行的 Redis 客户端。
    /// - `op`：计数结构与增减方向，方向不进入判重身份。
    /// - `key`：目标计数键，决定计数与账本的共同槽位。
    /// - `member`：字段或成员的原始命令字节；字符串计数使用空字节。
    /// - `delta`：已编码为 Redis 数值文本的步长字节。
    /// - `nonce`：稳定业务事件标识，不能为空或全空白。
    ///
    /// 返回：脚本首次结果字符串；跨 slot 派生、协议异常或拒绝码返回错误。
    async fn execute(
        &self,
        client: &RedisClient,
        op: Operation,
        key: &str,
        member: &[u8],
        delta: Vec<u8>,
        nonce: &str,
    ) -> Result<String> {
        let mode = self.resolved_mode(client).await?;
        let (keys, args, script, sha) = self.invocation(mode, op, key, member, delta, nonce)?;

        let mut conn = client.conn();
        let raw = eval_binary(&mut conn, script, sha, &keys, &args).await?;
        interpret_result(&self.metrics, &raw)
    }

    /// 业务作用：为已解析布局构造完整 Lua 调用，直发与 Pipeline 共用同一键、摘要和参数顺序。
    ///
    /// 参数说明：
    /// - `mode`：已确认的字段过期或环形桶布局，不接受尚未解析的自动模式。
    /// - `op`：脚本需要执行的计数结构与增减方向。
    /// - `key`：目标计数键，决定账本键派生和共同槽位。
    /// - `member`：字段或成员的原始命令字节，参与账本身份。
    /// - `delta`：供脚本数值命令使用的步长文本字节。
    /// - `nonce`：非空业务事件标识，与目标和成员共同确定判重范围。
    ///
    /// 返回：二进制 KEYS/ARGV、脚本文本与 SHA；空 nonce、跨 slot 或未解析模式返回错误且不发命令。
    fn invocation(
        &self,
        mode: IdempotentTtlMode,
        op: Operation,
        key: &str,
        member: &[u8],
        delta: Vec<u8>,
        nonce: &str,
    ) -> Result<IdempotentInvocation<'_>> {
        // 空 nonce 会让幂等方法退化成普通计数，必须在发命令前拒绝，不给资金路径留隐式漏洞。
        if nonce.trim().is_empty() {
            return Err(cfg("nonce 不能为空或全空白"));
        }
        let target_key = key.as_bytes();
        let expected_slot = redis_slot(target_key);
        let family = [op.family()];
        let record_field = wire::record_field(&family, member, nonce.as_bytes());
        let base = wire::ledger_base(
            &self.layout.ledger_key_prefix,
            target_key,
            &family,
            member,
            self.layout.ledger_shards,
        )
        .ok_or_else(|| protocol("idempotent ledger_shards is not a non-zero power of two"))?;
        let mut keys = vec![target_key.to_vec()];
        match mode {
            IdempotentTtlMode::HashField => {
                keys.push(self.ledger_key(base.into_bytes(), expected_slot)?);
            }
            IdempotentTtlMode::HashBucket => {
                for index in 0..self.layout.bucket_count {
                    keys.push(
                        self.ledger_key(format!("{base}:{index}").into_bytes(), expected_slot)?,
                    );
                }
            }
            IdempotentTtlMode::Auto => {
                return Err(protocol(
                    "idempotent runtime resolved to an unresolved AUTO mode",
                ));
            }
        }
        let args = vec![
            record_field.to_vec(),
            op.command().to_vec(),
            member.to_vec(),
            delta,
            self.layout.nonce_ttl_ms.to_string().into_bytes(),
            self.layout.bucket_span_ms.to_string().into_bytes(),
        ];
        let (script, sha) = match mode {
            IdempotentTtlMode::HashField => (HFE_SCRIPT, self.hfe_sha.as_str()),
            IdempotentTtlMode::HashBucket => (BUCKET_SCRIPT, self.bucket_sha.as_str()),
            IdempotentTtlMode::Auto => unreachable!("AUTO 已在上文拒绝"),
        };
        Ok((keys, args, script, sha))
    }

    /// 业务作用：构造可放入显式 Pipeline 的完整 EVAL 命令；每条命令仍独立完成原子判重与结算。
    ///
    /// 参数说明：
    /// - `op`：计数结构与增减方向，决定脚本分支。
    /// - `key`：目标计数键，决定账本键和槽位派生。
    /// - `member`：字段或成员的原始命令字节；字符串计数使用空字节。
    /// - `delta`：已编码的整数或浮点步长文本字节。
    /// - `nonce`：稳定业务事件标识，窗口内重复请求复用首次结果。
    ///
    /// 返回：布局已准备时返回单条 EVAL 命令；未准备、空 nonce 或同槽派生失败时返回错误。
    pub(crate) fn pipeline_command(
        &self,
        op: Operation,
        key: &str,
        member: &[u8],
        delta: Vec<u8>,
        nonce: &str,
    ) -> Result<redis::Cmd> {
        let mode = self
            .resolved
            .get()
            .copied()
            .ok_or_else(|| cfg("idempotent pipeline 创建前必须先准备账本布局"))?;
        let (keys, args, script, _) = self.invocation(mode, op, key, member, delta, nonce)?;
        let mut command = redis::cmd("EVAL");
        command.arg(script.as_bytes()).arg(keys.len());
        for key in keys {
            command.arg(key);
        }
        for arg in args {
            command.arg(arg);
        }
        Ok(command)
    }

    /// 业务作用：序列化账本键并复验其 slot，派生算法异常时在发命令前 fail-closed。
    ///
    /// 参数说明：
    /// - `key`: 账本键字节。
    /// - `expected_slot`: 目标业务键的实际 slot。
    ///
    /// 返回：同 slot 时返回该键；不一致返回协议错误，避免跨 slot 脚本错乱账本。
    fn ledger_key(&self, key: Vec<u8>, expected_slot: u16) -> Result<Vec<u8>> {
        if !wire::ledger_slot_matches(&key, expected_slot) {
            return Err(protocol(
                "derived idempotent ledger key is not in the target Redis slot",
            ));
        }
        Ok(key)
    }

    /// 业务作用：在接流量前主动解析并固定账本布局（读/建共享 marker + all-master 能力探测），把首个资金请求才会
    /// 暴露的布局冲突或能力缺失提前到 Ready 前暴露。
    ///
    /// 参数说明：
    /// - `client`: 承载 marker 与能力探测连接的 RedisClient。
    ///
    /// 返回：布局解析并复验成功返回；marker 冲突、配置不一致或所需能力缺失时返回错误并 fail-closed。
    /// 幂等：内部经 `OnceCell` 只解析一次，重复调用直接返回已固定结果。
    pub(crate) async fn prepare(&self, client: &RedisClient) -> Result<()> {
        self.resolved_mode(client).await.map(|_| ())
    }

    /// 业务作用：并发安全地取得当前客户端已经共享 marker 固定的幂等账本模式。
    ///
    /// 参数说明：
    /// - `client`：承载首次解析所需的共享布局标记读取及能力探测连接。
    ///
    /// 返回：已解析的 `HASH_FIELD`/`HASH_BUCKET`；探测不完整或配置冲突时返回错误并 fail-closed。
    async fn resolved_mode(&self, client: &RedisClient) -> Result<IdempotentTtlMode> {
        // OnceCell 保证并发首次调用只解析一次布局，避免多个请求各自竞争 marker。
        self.resolved
            .get_or_try_init(|| self.resolve_layout(client))
            .await
            .copied()
    }

    /// 业务作用：以共享 marker 固定 TTL 布局，防止不同应用节点把同一 nonce 写入两套账本。
    ///
    /// 参数说明：
    /// - `client`: 承载命令与探测连接的 RedisClient。
    ///
    /// 返回：与 Redis 共享 marker 一致的运行模式；能力或配置不一致时返回错误。
    async fn resolve_layout(&self, client: &RedisClient) -> Result<IdempotentTtlMode> {
        let marker_key = self.layout.marker_key();
        // marker 用原始字符串读写，不经业务 valueSerializer，保证不同配置节点能共享同一布局。
        if let Some(existing) = read_marker(client, &marker_key).await? {
            let mode = self.verify_marker(client, &existing).await?;
            self.metrics.resolve_layout(mode, &existing);
            return Ok(mode);
        }

        let resolved = match self.layout.ttl_mode {
            IdempotentTtlMode::HashBucket => IdempotentTtlMode::HashBucket,
            IdempotentTtlMode::HashField => {
                if !self.all_masters_support_hpexpire(client).await? {
                    return Err(cfg("HASH_FIELD 要求每个 master 都支持 HPEXPIRE"));
                }
                IdempotentTtlMode::HashField
            }
            IdempotentTtlMode::Auto => {
                if self.all_masters_support_hpexpire(client).await? {
                    IdempotentTtlMode::HashField
                } else {
                    IdempotentTtlMode::HashBucket
                }
            }
        };

        let candidate = self.layout.marker_value(resolved);
        // SET NX 竞争建标；即便本节点写入失败，也以回读到的实际 marker 为准，服从已有布局。
        let mut conn = client.conn();
        let _created: Option<String> = redis::cmd("SET")
            .arg(marker_key.as_bytes())
            .arg(candidate.as_bytes())
            .arg("NX")
            .query_async(&mut conn)
            .await
            .map_err(NasaRedisError::Redis)?;
        let actual = read_marker(client, &marker_key)
            .await?
            .ok_or_else(|| protocol("idempotent counter layout marker was not persisted"))?;
        let mode = self.verify_marker(client, &actual).await?;
        self.metrics.resolve_layout(mode, &actual);
        Ok(mode)
    }

    /// 业务作用：解析并复验共享布局；配置或服务端能力不一致时停止进入资金副作用路径。
    ///
    /// 参数说明：
    /// - `client`: 承载能力探测连接的 RedisClient。
    /// - `marker`: Redis 中保存的布局指纹。
    ///
    /// 返回：通过复验的运行模式；marker 版本、模式、参数或能力不符时返回错误。
    async fn verify_marker(&self, client: &RedisClient, marker: &str) -> Result<IdempotentTtlMode> {
        let parts: Vec<&str> = marker.split('|').collect();
        let (mode_index, ttl_index, span_index, shards_index) = match self.layout.profile {
            crate::config::CompatibilityProfile::LegacyV1 => {
                if parts.len() != 5 || parts[0] != "1" {
                    return Err(cfg("LegacyV1 需要版本 1 idempotent counter layout marker"));
                }
                (1, 2, 3, 4)
            }
            crate::config::CompatibilityProfile::RustV2 => {
                if parts.len() != 7
                    || parts[0] != "2"
                    || parts[1] != self.layout.ledger_key_prefix
                    || parts[2] != "record-sha256-v1"
                {
                    return Err(cfg("RustV2 idempotent counter layout marker 不兼容"));
                }
                (3, 4, 5, 6)
            }
        };
        let mode = IdempotentTtlMode::resolved_from_marker(parts[mode_index])
            .ok_or_else(|| cfg("layout marker 必须包含已解析的 TTL 模式"))?;
        let nonce_ttl: u64 = parts[ttl_index]
            .parse()
            .map_err(|_| cfg("layout marker nonceTtlMs 非法"))?;
        let bucket_span: u64 = parts[span_index]
            .parse()
            .map_err(|_| cfg("layout marker bucketSpanMs 非法"))?;
        let shards: u32 = parts[shards_index]
            .parse()
            .map_err(|_| cfg("layout marker ledgerShards 非法"))?;
        if nonce_ttl != self.layout.nonce_ttl_ms
            || bucket_span != self.layout.bucket_span_ms
            || shards != self.layout.ledger_shards
        {
            return Err(cfg(
                "本地 idempotent counter 配置与共享 layout marker 不一致",
            ));
        }
        if self.layout.ttl_mode != IdempotentTtlMode::Auto && self.layout.ttl_mode != mode {
            return Err(cfg("本地 ttl_mode 与共享 layout marker 不一致"));
        }
        // 字段级布局一旦由其它节点固定，本节点必须先证明自己连接的全部 master 都具备相同能力。
        if mode == IdempotentTtlMode::HashField
            && !self.all_masters_support_hpexpire(client).await?
        {
            return Err(cfg("共享 HASH_FIELD 布局要求每个 master 都支持 HPEXPIRE"));
        }
        Ok(mode)
    }

    /// 业务作用：判断当前拓扑的全部 master 是否都支持 HPEXPIRE，并把明确不支持与探测证据不完整分开。
    ///
    /// 参数说明：
    /// - `client`: 承载探测连接的 RedisClient。
    ///
    /// 返回：稳定拓扑全部确认支持时为 `true`，完整响应确认命令不存在时为 `false`；ACL、超时、拓扑变化或响应缺失返回错误，允许后续准备重试。
    async fn all_masters_support_hpexpire(&self, client: &RedisClient) -> Result<bool> {
        let outcome = self.probe_all_masters_hpexpire(client).await;
        if outcome.is_err() {
            self.metrics.record_probe_failure();
        }
        outcome
    }

    /// 业务作用：通过受管连接验证稳定 Cluster 拓扑的全部 master，避免部分连接成功被误判为全量能力。
    ///
    /// 参数说明：
    /// - `client`: 承载认证、TLS 与拓扑状态的 RedisClient。
    ///
    /// 返回：standalone 节点或稳定 Cluster 的全部 master 都确认支持时为 `true`；完整确认命令不存在时为 `false`；探测不完整返回错误。
    async fn probe_all_masters_hpexpire(&self, client: &RedisClient) -> Result<bool> {
        match client.conn() {
            Conn::Single(mut conn) => redis::cmd("COMMAND")
                .arg("INFO")
                .arg("HPEXPIRE")
                .query_async::<Value>(&mut conn)
                .await
                .map_err(NasaRedisError::Redis)
                .and_then(|value| {
                    // 只有明确的命令缺失才允许选择桶布局，畸形响应保留为可重新准备的错误。
                    supports_hpexpire_response(&value)
                        .ok_or_else(|| cfg("HPEXPIRE 能力探测返回了无效的命令定义"))
                }),
            Conn::Cluster(mut conn) => {
                const RETRY_DELAYS_MS: [u64; 5] = [0, 100, 250, 500, 1_000];
                // 拓扑变更与暂时缺失按有界时间退避重新取样；只有前后 master 集合稳定且响应地址完全覆盖才接受结论。
                for delay_ms in RETRY_DELAYS_MS {
                    if delay_ms > 0 {
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    }
                    let Some(before) = cluster_primary_addresses(&mut conn).await else {
                        continue;
                    };
                    let mut command = redis::cmd("COMMAND");
                    command.arg("INFO").arg("HPEXPIRE");
                    let responses = conn
                        .route_command(
                            command,
                            RoutingInfo::MultiNode((MultipleNodeRoutingInfo::AllMasters, None)),
                        )
                        .await;
                    let Some(after) = cluster_primary_addresses(&mut conn).await else {
                        continue;
                    };
                    if before != after {
                        continue;
                    }
                    let Ok(value) = responses else {
                        continue;
                    };
                    match cluster_response_coverage(&value, &before) {
                        ClusterProbeCoverage::Supported => return Ok(true),
                        ClusterProbeCoverage::Unsupported => return Ok(false),
                        ClusterProbeCoverage::Incomplete => continue,
                    }
                }
                Err(cfg("HPEXPIRE 能力探测未取得稳定拓扑的全量 master 响应"))
            }
        }
    }
}

/// 业务作用：从同一受管 Cluster 连接读取 distinct primary 地址集合，供能力响应做全覆盖复验。
///
/// 参数说明：
/// - `conn`: 已完成认证、TLS 与拓扑发现的 Cluster 连接。
///
/// 返回：完整且非空的 primary 地址集合；协议结构不完整时返回 `None`，调用方按能力未知处理。
async fn cluster_primary_addresses(
    conn: &mut redis::cluster_async::ClusterConnection,
) -> Option<BTreeSet<String>> {
    let mut command = redis::cmd("CLUSTER");
    command.arg("SLOTS");
    let value = conn
        .route_command(
            command,
            RoutingInfo::SingleNode(SingleNodeRoutingInfo::RandomPrimary),
        )
        .await
        .ok()?;
    let Value::Array(rows) = value else {
        return None;
    };
    let mut primaries = BTreeSet::new();
    for row in rows {
        let Value::Array(fields) = row else {
            return None;
        };
        let Some(Value::Array(primary)) = fields.get(2) else {
            return None;
        };
        let host = redis_text(primary.first()?)?;
        let port = match primary.get(1)? {
            Value::Int(value) if (1..=u16::MAX as i64).contains(value) => *value as u16,
            _ => return None,
        };
        if host == "?" {
            return None;
        }
        let address = if host.is_empty() || matches!(host.as_str(), "0.0.0.0" | "::" | "[::]") {
            format!("*:{port}")
        } else {
            format!("{host}:{port}")
        };
        primaries.insert(address);
    }
    (!primaries.is_empty()).then_some(primaries)
}

/// 业务作用：复验 AllMasters 返回 map 与拓扑地址集合一一对应，并区分命令缺失与响应覆盖不完整。
///
/// 参数说明：
/// - `value`: AllMasters 聚合响应。
/// - `expected`: 探测前后稳定的 primary 地址集合。
///
/// 返回：地址和有效命令响应完全覆盖后给出支持或不支持；节点重复、缺失、额外、地址歧义或畸形定义均返回不完整。
fn cluster_response_coverage(value: &Value, expected: &BTreeSet<String>) -> ClusterProbeCoverage {
    let Value::Map(entries) = value else {
        return ClusterProbeCoverage::Incomplete;
    };
    let mut answered = BTreeSet::new();
    let mut supported = true;
    for (address, response) in entries {
        let Some(address) = redis_text(address) else {
            return ClusterProbeCoverage::Incomplete;
        };
        if !answered.insert(address) {
            return ClusterProbeCoverage::Incomplete;
        }
        // 任一节点能力未知都不能固定共享布局，即使其它节点明确不支持也要先取得完整证据。
        let Some(node_supported) = supports_hpexpire_response(response) else {
            return ClusterProbeCoverage::Incomplete;
        };
        supported &= node_supported;
    }
    if !addresses_cover(expected, &answered) {
        return ClusterProbeCoverage::Incomplete;
    }
    if supported {
        ClusterProbeCoverage::Supported
    } else {
        ClusterProbeCoverage::Unsupported
    }
}

/// 业务作用：把 CLUSTER SLOTS 的 wildcard 主机按端口与 AllMasters 实际地址唯一匹配，复验两边节点集合等价。
///
/// 参数说明：
/// - `expected`：拓扑宣告的主节点地址集合，未知主机以端口通配形式表示。
/// - `answered`：逐主节点路由返回的实际地址集合，不允许多余或缺少节点。
///
/// 返回：每个宣告节点都能一对一匹配且没有额外响应时为 `true`；端口歧义或集合差异为 `false`。
fn addresses_cover(expected: &BTreeSet<String>, answered: &BTreeSet<String>) -> bool {
    if expected.len() != answered.len() {
        return false;
    }
    let mut remaining = answered.clone();
    for address in expected {
        if remaining.remove(address) {
            continue;
        }
        let Some(port) = address.strip_prefix("*:") else {
            return false;
        };
        let candidates: Vec<String> = remaining
            .iter()
            .filter(|candidate| address_port(candidate).is_some_and(|value| value == port))
            .cloned()
            .collect();
        if candidates.len() != 1 {
            return false;
        }
        remaining.remove(&candidates[0]);
    }
    remaining.is_empty()
}

/// 业务作用：从 AllMasters 节点地址提取端口文本，兼容 IPv4、主机名与带方括号 IPv6。
///
/// 参数说明：
/// - `address`：redis-rs 路由响应中的节点地址，用于按末段端口匹配未知主机。
///
/// 返回：存在非空末段端口时返回文本，否则返回 `None`。
fn address_port(address: &str) -> Option<&str> {
    let (_, port) = address.rsplit_once(':')?;
    (!port.is_empty()).then_some(port)
}

/// 业务作用：区分单节点 HPEXPIRE 能力证据、明确命令缺失与无效响应，防止未知状态固定账本布局。
///
/// 参数说明：
/// - `value`: 节点原始响应。
///
/// 返回：有效命令定义为 `Some(true)`，唯一 nil 项为 `Some(false)`；错误、名称不符或结构异常为 `None`。
fn supports_hpexpire_response(value: &Value) -> Option<bool> {
    let Value::Array(items) = value else {
        return None;
    };
    // 单命令探测只能有一个对应结果；额外结果不能作为当前命令的能力证据。
    let [command] = items.as_slice() else {
        return None;
    };
    match command {
        Value::Nil => Some(false),
        Value::Array(fields) if fields.len() >= 6 => {
            let name = redis_text(&fields[0])?;
            if !name.eq_ignore_ascii_case("hpexpire")
                || !matches!(fields[1], Value::Int(arity) if arity != 0)
                || !matches!(&fields[2], Value::Array(flags) if flags.iter().all(|flag| matches!(flag, Value::BulkString(_) | Value::SimpleString(_))))
                || !fields[3..6]
                    .iter()
                    .all(|field| matches!(field, Value::Int(position) if *position >= 0))
            {
                // 无效定义意味着能力未知，不能据此建成任何永久布局标记。
                return None;
            }
            Some(true)
        }
        _ => None,
    }
}

/// 业务作用：读取 Redis 文本值而不进行有损替换，节点地址不合法时拒绝覆盖判定。
///
/// 参数说明：
/// - `value`: bulk 或 simple Redis 字符串。
///
/// 返回：有效 UTF-8 文本；其它类型或非法 UTF-8 返回 `None`。
fn redis_text(value: &Value) -> Option<String> {
    match value {
        Value::BulkString(bytes) => String::from_utf8(bytes.clone()).ok(),
        Value::SimpleString(text) => Some(text.clone()),
        _ => None,
    }
}

/// 业务作用：把脚本三元素结果解释为首次业务值，并让直发与 Pipeline 共享同一拒绝及观测语义。
///
/// 参数说明：
/// - `metrics`: 当前客户端的幂等计数观测容器。
/// - `raw`: 脚本返回的 `{code, value, detail}`。
///
/// 返回：成功或重复时返回首次值文本；结构化拒绝、未知码或畸形结构返回错误。
pub(crate) fn interpret_result(
    metrics: &IdempotentCounterMetrics,
    raw: &[Value],
) -> Result<String> {
    if raw.len() != 3 {
        return Err(protocol(
            "idempotent counter script returned an invalid result",
        ));
    }
    let code_text = value_to_string(&raw[0]);
    let value = value_to_string(&raw[1]);
    let detail = value_to_string(&raw[2]);
    let code = IdempotentResultCode::parse(&code_text)
        .ok_or_else(|| protocol("idempotent counter script returned an unknown result code"))?;
    match code {
        IdempotentResultCode::Applied => {
            metrics.record_applied();
            Ok(value)
        }
        IdempotentResultCode::AppliedTtlMissing => {
            // 凭证已经落账但 TTL 未确认，不能删除凭证放开重放；保留成功结局并把回收风险暴露为降级健康。
            metrics.record_applied();
            metrics.record_ttl_missing();
            tracing::error!(detail = %detail, "idempotent counter credential TTL was not confirmed");
            Ok(value)
        }
        IdempotentResultCode::Duplicate => {
            metrics.record_duplicate();
            Ok(value)
        }
        IdempotentResultCode::RejectedLedgerType => {
            metrics.record_rejected(IdempotentRejection::LedgerType);
            Err(reject(IdempotentRejection::LedgerType, detail))
        }
        IdempotentResultCode::RejectedOperation => {
            metrics.record_rejected(IdempotentRejection::Operation);
            Err(reject(IdempotentRejection::Operation, detail))
        }
        IdempotentResultCode::RejectedCommand => {
            metrics.record_rejected(IdempotentRejection::Command);
            Err(reject(IdempotentRejection::Command, detail))
        }
    }
}

/// 业务作用：用二进制 KEYS/ARGV 执行幂等脚本，优先 EVALSHA，收到 NOSCRIPT 后对同一连接回退 EVAL。
///
/// 参数说明：
/// - `conn`: 目标命令连接。
/// - `script`：目标节点缺少脚本缓存时使用的完整 Lua 文本。
/// - `sha`：与脚本文本对应的 SHA1，供首次 EVALSHA 请求使用。
/// - `keys`：按脚本约定排列的二进制键，必须处于同一槽位。
/// - `args`：已序列化的业务参数，保留字段、成员和数值文本的原始字节。
///
/// 返回：脚本返回的原始 `Value` 数组；网络或协议错误透传，NOSCRIPT 不视为业务错误。
async fn eval_binary(
    conn: &mut Conn,
    script: &str,
    sha: &str,
    keys: &[Vec<u8>],
    args: &[Vec<u8>],
) -> Result<Vec<Value>> {
    match run_eval(conn, "EVALSHA", sha.as_bytes(), keys, args).await {
        Ok(values) => Ok(values),
        Err(error) if is_noscript(&error) => {
            // 目标节点尚未缓存脚本：对同一连接回退整文本 EVAL，不依赖一次 SCRIPT LOAD 广播到全部节点。
            run_eval(conn, "EVAL", script.as_bytes(), keys, args)
                .await
                .map_err(script_execution_error)
        }
        Err(error) => Err(script_execution_error(error)),
    }
}

/// 业务作用：把脚本传输中断归类为执行结果未知，禁止上层把可能已结算的请求当作确定未执行。
///
/// 参数说明：
/// - `error`: EVALSHA 或 EVAL 返回的底层错误。
///
/// 返回：连接中断、I/O 或超时映射为 `ExecutionUnknown`；确定性服务端拒绝保留 Redis 错误类型。
fn script_execution_error(error: redis::RedisError) -> NasaRedisError {
    if crate::error::redis_is_transient(&error) {
        NasaRedisError::ExecutionUnknown(
            "幂等计数脚本已发送，但连接在确认结果前中断；请使用原 nonce 重试".to_owned(),
        )
    } else {
        NasaRedisError::Redis(error)
    }
}

/// 业务作用：组装并执行一次 EVAL/EVALSHA，保持二进制键与参数不经 UTF-8 转换。
///
/// 参数说明：
/// - `conn`：承载脚本命令的受管连接。
/// - `verb`：调用方选择的 EVAL 或 EVALSHA 命令名。
/// - `body`：与命令名对应的脚本文本或缓存摘要字节。
/// - `keys`：按脚本顺序传入的二进制键，同时确定命令路由。
/// - `args`：按脚本顺序传入的非键参数字节。
///
/// 返回：类型化为 `Vec<Value>` 的脚本返回；底层错误透传由调用方分类。
async fn run_eval(
    conn: &mut Conn,
    verb: &str,
    body: &[u8],
    keys: &[Vec<u8>],
    args: &[Vec<u8>],
) -> std::result::Result<Vec<Value>, redis::RedisError> {
    let mut cmd = redis::cmd(verb);
    cmd.arg(body).arg(keys.len());
    for key in keys {
        cmd.arg(key.as_slice());
    }
    for arg in args {
        cmd.arg(arg.as_slice());
    }
    cmd.query_async(conn).await
}

/// 业务作用：判断错误是否为服务端 NOSCRIPT，用于精确触发脚本加载回退。
///
/// 参数说明：
/// - `error`: EVALSHA 返回的错误。
///
/// 返回：错误类别为 NOSCRIPT 时为 `true`；其余错误保持透传，不做加载回退。
fn is_noscript(error: &redis::RedisError) -> bool {
    error.kind() == ErrorKind::Server(ServerErrorKind::NoScript)
}

/// 业务作用：读取原始字符串 marker，不经业务 valueSerializer。
///
/// 参数说明：
/// - `client`: 承载命令连接的 RedisClient。
/// - `marker_key`: marker 文本键。
///
/// 返回：存在时返回 UTF-8 文本；不存在返回 `None`；非 UTF-8 视为协议错误。
async fn read_marker(client: &RedisClient, marker_key: &str) -> Result<Option<String>> {
    let mut conn = client.conn();
    let raw: Option<Vec<u8>> = redis::cmd("GET")
        .arg(marker_key.as_bytes())
        .query_async(&mut conn)
        .await
        .map_err(NasaRedisError::Redis)?;
    match raw {
        None => Ok(None),
        Some(bytes) => String::from_utf8(bytes)
            .map(Some)
            .map_err(|_| protocol("idempotent counter layout marker is not valid UTF-8")),
    }
}

/// 业务作用：把脚本返回的单个 `Value` 归一化为字符串，兼容整数、bulk、simple 与 nil。
///
/// 参数说明：
/// - `value`: 脚本返回数组的一个元素。
///
/// 返回：整数与字符串取其文本，nil 与其它类型取空串。
fn value_to_string(value: &Value) -> String {
    match value {
        Value::Nil => String::new(),
        Value::Int(n) => n.to_string(),
        Value::BulkString(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        Value::SimpleString(text) => text.clone(),
        _ => String::new(),
    }
}

/// 业务作用：解析 Redis 浮点协议，Redis 用 inf/-inf 表示无穷。
///
/// 参数说明：
/// - `value`: ZINCRBY 返回文本。
///
/// 返回：对应 double（含 ±inf）；非数值返回协议错误。
fn parse_redis_double(value: &str) -> Result<f64> {
    let lower = value.to_ascii_lowercase();
    match lower.as_str() {
        "inf" | "+inf" => Ok(f64::INFINITY),
        "-inf" => Ok(f64::NEG_INFINITY),
        _ => value
            .parse::<f64>()
            .map_err(|_| protocol("idempotent ZSET script returned a non-numeric value")),
    }
}

/// 业务作用：构造幂等配置类错误。
///
/// 参数说明：
/// - `message`：账本配置或能力准备失败的原因摘要。
///
/// 返回：保留摘要的配置错误，由调用方停止本次账本准备或命令构造。
fn cfg(message: &str) -> NasaRedisError {
    NasaRedisError::Config(message.to_owned())
}

/// 业务作用：构造幂等协议类错误（结果结构或类型不符合脚本合同）。
///
/// 参数说明：
/// - `message`：不符合账本或脚本协议的原因摘要。
///
/// 返回：独立的幂等协议错误，不能解释为计数已确定未执行。
fn protocol(message: &str) -> NasaRedisError {
    NasaRedisError::IdempotentProtocol(message.to_owned())
}

/// 业务作用：把结构化拒绝封装为统一错误类型，业务分支基于 `IdempotentRejection`。
///
/// 参数说明：
/// - `code`：已经解析的封闭拒绝类别。
/// - `detail`：脚本返回的诊断文本，对外展示前须检查是否需要脱敏。
///
/// 返回：包含类别及诊断详情的幂等拒绝错误，不在此处重试命令。
fn reject(code: IdempotentRejection, detail: String) -> NasaRedisError {
    NasaRedisError::Idempotent(IdempotentCounterError {
        code,
        detail,
    })
}
