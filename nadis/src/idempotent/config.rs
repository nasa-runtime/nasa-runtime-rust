//! nonce 幂等计数配置：账本布局参数在 client 建立后冻结，运行期不得切换，避免同一 nonce 落到两套账本。

use serde::Deserialize;

use crate::config::CompatibilityProfile;
use crate::error::{NasaRedisError, Result};

/// canonical slot token 表长度，同时是 `ledger_shards` 与桶数的绝对上界参照。
const MAX_LEDGER_SHARDS: u32 = 4_096;
/// 单个逻辑计数器允许的最大环形桶数。
const MAX_BUCKET_COUNT: u64 = 512;
/// 默认 nonce 保证窗口 7 天。
const DEFAULT_NONCE_TTL_MS: u64 = 7 * 24 * 60 * 60 * 1_000;
/// 默认单桶跨度 1 天。
const DEFAULT_BUCKET_SPAN_MS: u64 = 24 * 60 * 60 * 1_000;

/// nonce 凭证的 TTL 布局模式；决定账本 key 结构与回收机制，一旦由 marker 固定即不可变。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IdempotentTtlMode {
    /// 由全部 master 的 `HPEXPIRE` 能力决定：支持则 `HASH_FIELD`，否则 `HASH_BUCKET`。
    Auto,
    /// 每个 nonce 凭证用 `HPEXPIRE` 独立回收；要求所有 master 支持字段级 TTL（Redis 7.4+）。
    HashField,
    /// 环形桶按整桶换代回收，不依赖字段级 TTL。
    HashBucket,
}

impl IdempotentTtlMode {
    /// 业务作用：返回写入 layout marker 的稳定协议文本，其它节点据此复验布局一致。
    ///
    /// 参数说明：`profile` 决定 marker 的跨语言或 Rust 独立线格式。
    ///
    /// 返回：`AUTO`/`HASH_FIELD`/`HASH_BUCKET`；文本是持久协议的一部分，不可改名。
    pub(crate) fn marker_token(self) -> &'static str {
        match self {
            Self::Auto => "AUTO",
            Self::HashField => "HASH_FIELD",
            Self::HashBucket => "HASH_BUCKET",
        }
    }

    /// 业务作用：从 marker 协议文本还原已解析的 TTL 模式，未知或未解析（`AUTO`）时拒绝。
    ///
    /// 参数说明：
    /// - `token`: marker 第二段文本。
    ///
    /// 返回：`HASH_FIELD`/`HASH_BUCKET` 时返回对应模式；`AUTO` 或未知返回 `None`，调用方 fail-closed。
    pub(crate) fn resolved_from_marker(token: &str) -> Option<Self> {
        match token {
            "HASH_FIELD" => Some(Self::HashField),
            "HASH_BUCKET" => Some(Self::HashBucket),
            _ => None,
        }
    }
}

/// 幂等计数原始配置，挂在 `redis.idempotent_counter` 段下；缺省时使用保守默认。
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IdempotentCounterCfg {
    /// 账本 key 前缀；与既有 `rpidem` 数据共用同一 Redis 边界时保持默认前缀。
    pub ledger_key_prefix: String,
    /// layout marker 的逻辑数据源标识，决定 marker 键；不同逻辑 Redis 数据源必须用不同值。
    pub layout_marker_id: String,
    /// nonce 保证窗口毫秒数；窗口内同一 nonce 只结算一次。
    pub nonce_ttl_ms: u64,
    /// 布局模式。
    pub ttl_mode: IdempotentTtlMode,
    /// 单个环形桶跨度毫秒数（仅 `HASH_BUCKET` 使用，但进入 marker 参与一致性校验）。
    pub bucket_span_ms: u64,
    /// ledger shard 数量，必须为 `1..=4096` 的 2 的幂。
    pub ledger_shards: u32,
}

impl Default for IdempotentCounterCfg {
    /// 业务作用：提供连接、窗口与桶都有界的默认账本布局。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：`rpidem`/`primary`/7 天窗口/`AUTO`/1 天桶/256 shard 的默认配置。
    fn default() -> Self {
        Self {
            ledger_key_prefix: "rpidem".to_owned(),
            layout_marker_id: "primary".to_owned(),
            nonce_ttl_ms: DEFAULT_NONCE_TTL_MS,
            ttl_mode: IdempotentTtlMode::Auto,
            bucket_span_ms: DEFAULT_BUCKET_SPAN_MS,
            ledger_shards: 256,
        }
    }
}

impl IdempotentCounterCfg {
    /// 业务作用：把原始配置校验并冻结为运行期快照；桶数按窗口与桶跨度派生，越界在使用前拒绝。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：全部约束通过时返回不可变快照；前缀非法、参数非正、shard 非 2 的幂或桶数越界返回配置错误。
    pub(crate) fn snapshot(
        &self,
        profile: CompatibilityProfile,
    ) -> Result<IdempotentLayoutSnapshot> {
        let prefix = self.ledger_key_prefix.trim();
        if prefix.is_empty() || prefix.len() > 128 {
            return Err(cfg(
                "idempotent_counter.ledger_key_prefix 必须为 1..=128 字节的非空文本",
            ));
        }
        if !prefix
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-'))
        {
            return Err(cfg(
                "idempotent_counter.ledger_key_prefix 只能包含 ASCII 字母数字与 '.' '_' ':' '-'",
            ));
        }
        // Java 的 marker value 不携带 prefix；跨语言 profile 只能使用固定前缀，否则两个节点会各自建标并重复结算。
        if profile == CompatibilityProfile::LegacyV1 && prefix != "rpidem" {
            return Err(cfg(
                "LegacyV1 的 idempotent_counter.ledger_key_prefix 必须为 rpidem",
            ));
        }
        let marker_id = self.layout_marker_id.trim();
        if marker_id.is_empty() || marker_id.len() > 128 {
            return Err(cfg(
                "idempotent_counter.layout_marker_id 必须为 1..=128 字节的非空文本",
            ));
        }
        if self.nonce_ttl_ms < 1 || self.nonce_ttl_ms > i64::MAX as u64 {
            return Err(cfg("idempotent_counter.nonce_ttl_ms 必须位于 1..=i64::MAX"));
        }
        if self.bucket_span_ms < 1 {
            return Err(cfg("idempotent_counter.bucket_span_ms 必须大于 0"));
        }
        if self.ledger_shards < 1
            || self.ledger_shards > MAX_LEDGER_SHARDS
            || (self.ledger_shards & (self.ledger_shards - 1)) != 0
        {
            return Err(cfg(
                "idempotent_counter.ledger_shards 必须为 1..=4096 的 2 的幂",
            ));
        }
        // 桶数由 `(nonce_ttl_ms-1)/bucket_span_ms + 2` 派生，是持久布局的一部分：HASH_BUCKET 的账本 key
        // 集合由它决定，桶数不一致会写入不同数量的桶，破坏环形代次判重。
        let complete_spans = (self.nonce_ttl_ms - 1) / self.bucket_span_ms;
        if complete_spans > MAX_BUCKET_COUNT - 2 {
            return Err(cfg(
                "idempotent_counter.nonce_ttl_ms 与 bucket_span_ms 组合需要超过 512 个环形桶",
            ));
        }
        let bucket_count = (complete_spans + 2) as u32;
        Ok(IdempotentLayoutSnapshot {
            ledger_key_prefix: prefix.to_owned(),
            layout_marker_id: marker_id.to_owned(),
            profile,
            nonce_ttl_ms: self.nonce_ttl_ms,
            ttl_mode: self.ttl_mode,
            bucket_span_ms: self.bucket_span_ms,
            ledger_shards: self.ledger_shards,
            bucket_count,
        })
    }
}

/// 已校验并冻结的账本布局；跨节点一致性由 layout marker 保证。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IdempotentLayoutSnapshot {
    pub(crate) ledger_key_prefix: String,
    pub(crate) layout_marker_id: String,
    pub(crate) profile: CompatibilityProfile,
    pub(crate) nonce_ttl_ms: u64,
    pub(crate) ttl_mode: IdempotentTtlMode,
    pub(crate) bucket_span_ms: u64,
    pub(crate) ledger_shards: u32,
    pub(crate) bucket_count: u32,
}

impl IdempotentLayoutSnapshot {
    /// 业务作用：生成 layout marker 的跨节点稳定协议文本；已解析模式写入，`AUTO` 由调用方先解析。
    ///
    /// 参数说明：
    /// - `resolved`: 能力探测后确定的 `HASH_FIELD`/`HASH_BUCKET`。
    ///
    /// 返回：跨语言 profile 使用版本 1；Rust 独立 profile 使用包含前缀和摘要算法的版本 2。
    pub(crate) fn marker_value(&self, resolved: IdempotentTtlMode) -> String {
        match self.profile {
            CompatibilityProfile::LegacyV1 => format!(
                "1|{}|{}|{}|{}",
                resolved.marker_token(),
                self.nonce_ttl_ms,
                self.bucket_span_ms,
                self.ledger_shards
            ),
            CompatibilityProfile::RustV2 => format!(
                "2|{}|record-sha256-v1|{}|{}|{}|{}",
                self.ledger_key_prefix,
                resolved.marker_token(),
                self.nonce_ttl_ms,
                self.bucket_span_ms,
                self.ledger_shards
            ),
        }
    }

    /// 业务作用：返回 layout marker 的键文本；不同 `layout_marker_id` 隔离不同逻辑数据源的布局。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：跨语言 profile 使用 Java marker key；Rust 独立 profile 使用不随账本前缀变化的协调键。
    pub(crate) fn marker_key(&self) -> String {
        match self.profile {
            CompatibilityProfile::LegacyV1 => {
                format!(
                    "{}:layout:{}",
                    self.ledger_key_prefix, self.layout_marker_id
                )
            }
            CompatibilityProfile::RustV2 => {
                format!("nasa:idempotent-counter:layout:{}", self.layout_marker_id)
            }
        }
    }
}

/// 业务作用：构造带 `idempotent_counter` 前缀的配置错误，便于业务定位账本布局问题。
///
/// 参数说明：
/// - `message`: 稳定错误摘要。
///
/// 返回：`NasaRedisError::Config` 错误。
fn cfg(message: &str) -> NasaRedisError {
    NasaRedisError::Config(message.to_owned())
}
