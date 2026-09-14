//! nonce 幂等计数的账本身份线协议核心：账本身份摘要、ledger shard、canonical slot token 与 ledger key 派生。
//!
//! 本模块的每个输出都进入共享 Redis 账本，是持久线协议，字节布局必须长期稳定：同一个业务 nonce 只有在
//! 目标 key、结构 family、field/member 与 nonce 的命令字节完全相同时才命中同一条凭证。任何摘要或 key
//! 派生的漂移都会让同一 nonce 落到不同账本，破坏“重复 nonce 只结算一次”的资金安全不变量。

use std::sync::OnceLock;

use sha2::{Digest, Sha256};

use crate::keytag::redis_slot;

/// Redis Cluster 的固定 slot 总数。
pub const SLOT_COUNT: usize = 16_384;

/// 不区分方向的逻辑计数器结构身份；进入 record field 与 ledger shard 摘要，值固定不可变。
pub const FAMILY_STRING: u8 = 1;
/// Hash field 计数器的持久结构类别。
pub const FAMILY_HASH: u8 = 2;
/// Sorted Set member 计数器的持久结构类别。
pub const FAMILY_ZSET: u8 = 3;

/// 业务作用：以 4 字节大端长度前缀拼接多个二进制身份段再取 SHA-256，防止 field、member 或 nonce 内容
/// 形成拼接歧义。
///
/// 参数说明：
/// - `parts`: 有序身份段；顺序、长度前缀与内容都进入摘要。
///
/// 返回：SHA-256 原始 32 字节摘要；长度前缀 + 内容的组合是持久身份合同的一部分。
pub fn framed_digest(parts: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for part in parts {
        let len = part.len() as u32;
        // 长度前缀必须先于内容写入：否则 ("ab","")与("a","b")会得到相同摘要，出现账本身份碰撞。
        hasher.update(len.to_be_bytes());
        hasher.update(part);
    }
    hasher.finalize().into()
}

/// 业务作用：对目标 key 的实际命令字节直接取 SHA-256，作为 ledger key 中的 target 段。
///
/// 参数说明：
/// - `target_key`: 发往 Redis 的目标键原始字节。
///
/// 返回：不含长度前缀的 SHA-256 原始 32 字节。
pub fn raw_digest(target_key: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(target_key);
    hasher.finalize().into()
}

/// 业务作用：取目标 key 的 SHA-256 小写 hex，作为 ledger key 的 target 段文本。
///
/// 参数说明：
/// - `target_key`: 目标键原始字节。
///
/// 返回：64 位小写 hex。
pub fn target_digest_hex(target_key: &[u8]) -> String {
    hex::encode(raw_digest(target_key))
}

/// 业务作用：按结构 family 与 field/member 选择固定 ledger shard；nonce 不参与，使同一逻辑计数器的历史桶
/// 可一次查询。
///
/// 参数说明：
/// - `family`: 结构身份字节（`FAMILY_*`）。
/// - `member`: Hash field 或 ZSet member 的命令字节；String 计数器为空。
/// - `shards`: `ledger_shards`，调用方保证为 2 的幂。
///
/// 返回：参数为非零 2 的幂时返回 `0..shards` 的 shard 下标；其它值返回 `None`，不会发生下溢或生成无效布局。
pub fn ledger_shard(family: &[u8], member: &[u8], shards: u32) -> Option<u32> {
    if shards == 0 || !shards.is_power_of_two() {
        return None;
    }
    let value = framed_digest(&[family, member]);
    // 取摘要前四字节大端；`shards` 为 2 的幂时低位掩码结果非负且落在 0..shards。
    let hash = (u32::from(value[0]) << 24)
        | (u32::from(value[1]) << 16)
        | (u32::from(value[2]) << 8)
        | u32::from(value[3]);
    Some(hash & (shards - 1))
}

/// 业务作用：派生 nonce 凭证的 record field（HSET 的二进制字段名），只含结构身份、field/member 与 nonce。
///
/// 参数说明：
/// - `family`: 结构身份字节；方向不参与，使同一 nonce 在增减漂移时仍命中首次结果。
/// - `member`: Hash field 或 ZSet member 命令字节；String 为空。
/// - `nonce`: 业务 nonce 的 UTF-8 字节。
///
/// 返回：SHA-256 原始 32 字节；family、member 与 nonce 的 UTF-8 字节按长度前缀顺序进入摘要。
pub fn record_field(family: &[u8], member: &[u8], nonce: &[u8]) -> [u8; 32] {
    framed_digest(&[family, member, nonce])
}

/// 业务作用：生成覆盖全部 16384 个 slot 的确定 canonical token 表，使任意目标 key 都能派生同 slot 的账本 key。
///
/// 参数说明: 无。
///
/// 返回：按 slot 下标排列、每项 `"s"+base36(candidate)` 的 token 表；枚举顺序固定，值随协议长期稳定。
pub fn slot_tokens() -> &'static [String; SLOT_COUNT] {
    static TOKENS: OnceLock<Box<[String; SLOT_COUNT]>> = OnceLock::new();
    TOKENS
        .get_or_init(|| {
            // 枚举顺序与 first-come 分配是持久协议：一旦改变，同一 slot 会得到不同 token，
            // 派生出的账本 key 与既有数据不在同一条凭证上。
            let mut tokens: Vec<String> = vec![String::new(); SLOT_COUNT];
            let mut assigned = vec![false; SLOT_COUNT];
            let mut remaining = SLOT_COUNT;
            let mut candidate: u64 = 0;
            while remaining > 0 {
                let token = format!("s{}", base36(candidate));
                candidate += 1;
                let slot = redis_slot(token.as_bytes()) as usize;
                if !assigned[slot] {
                    assigned[slot] = true;
                    tokens[slot] = token;
                    remaining -= 1;
                }
            }
            let array: Box<[String; SLOT_COUNT]> = tokens
                .into_boxed_slice()
                .try_into()
                .expect("slot token 表长度恒为 SLOT_COUNT");
            array
        })
        .as_ref()
}

/// 业务作用：把无符号整数编码为最短小写 base36（`0-9a-z`）文本，供 slot token 枚举使用。
///
/// 参数说明：
/// - `value`: 待编码的非负整数。
///
/// 返回：`0-9a-z` 组成的最短 base36 文本；`0` 返回 `"0"`。
fn base36(value: u64) -> String {
    const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    if value == 0 {
        return "0".to_owned();
    }
    let mut n = value;
    let mut buf = Vec::new();
    while n > 0 {
        buf.push(DIGITS[(n % 36) as usize]);
        n /= 36;
    }
    buf.reverse();
    String::from_utf8(buf).expect("base36 字符集恒为 ASCII")
}

/// 业务作用：派生某目标 key 对应的 ledger base 文本（不含桶下标），并保证其与目标 key 落在同一 slot。
///
/// 参数说明：
/// - `prefix`: 配置的 `ledger_key_prefix`。
/// - `target_key`: 目标键命令字节。
/// - `family`/`member`: 结构身份与 field/member 命令字节。
/// - `shards`: `ledger_shards`（2 的幂）。
///
/// 返回：shard 配置合法时返回 `prefix:{slot-token}:target-hex:shard` 文本；否则返回 `None`。
pub fn ledger_base(
    prefix: &str,
    target_key: &[u8],
    family: &[u8],
    member: &[u8],
    shards: u32,
) -> Option<String> {
    let slot = redis_slot(target_key) as usize;
    let token = &slot_tokens()[slot];
    let target_hex = target_digest_hex(target_key);
    let shard = ledger_shard(family, member, shards)?;
    Some(format!("{prefix}:{{{token}}}:{target_hex}:{shard}"))
}

/// 业务作用：复验一条已派生的账本 key 与目标 slot 一致，派生算法异常时在发命令前 fail-closed。
///
/// 参数说明：
/// - `ledger_key`: 已拼接的账本 key 字节。
/// - `expected_slot`: 目标业务 key 的实际 slot。
///
/// 返回：同 slot 时为 `true`；不一致返回 `false`，调用方必须拒绝发命令，避免跨 slot 脚本错乱账本。
pub fn ledger_slot_matches(ledger_key: &[u8], expected_slot: u16) -> bool {
    redis_slot(ledger_key) == expected_slot
}
