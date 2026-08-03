// ============================================================================
// src/keytag.rs —— slot 派生:逐字节复刻 Redis 算法(文档/ 定稿)。
//
// 红线:派生 key **按 slot 计算,不按字符串规则推断**——
//   · Redis 有效 hash tag = 第一个 '{' + 其后第一个 '}',内容非空才生效;
//     空 `{}` → 整 key 哈希,**不找后续 tag**(实测 slot("foo{}{bar}")=8363 ≠ slot("{bar}")=5061);
//   · 含括号 key 包一层会被第一个 '}' 截断(slot("{foo{}{bar}}:q")=7673);
//   · 无生效 tag 的 key 派生辅助 key:用 synthetic tag 表(0..16383 全覆盖,固定算法 v1,
//     每项构造时断言 CRC16(tag)%16384==slot)。
// source digest(防同 slot 不同 source 碰撞)属 R4 disposition/lease key,届时引入
// ≥128 位稳定摘要;本模块保持零外部依赖。
// ============================================================================

use std::sync::OnceLock;

/// FNV-1a 64-bit 稳定摘要(跨进程/跨版本一致;**绝不用随机 seed 的 HashMap hasher**——
/// 那会让持久化 key 成分每进程不同)。用于 operation_id 等需要稳定确定性摘要的场景。
/// 返回 16 位十六进制字符串。
///
/// # 参数
/// - `data`: 要参与稳定摘要计算的原始字节。
pub fn fnv1a64_hex(data: &[u8]) -> String {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut h = OFFSET;
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(PRIME);
    }
    format!("{h:016x}")
}

/// CRC16-XMODEM(Redis CLUSTER KEYSLOT 同款,多项式 0x1021,初值 0)。
///
/// # 参数
/// - `data`: 要计算 Redis slot CRC 的原始字节。
pub fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &b in data {
        crc ^= (b as u16) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
        }
    }
    crc
}

/// Redis 有效 hash tag 提取:第一个 '{' 与其后第一个 '}' 之间**非空**才生效。
/// 返回 None = 无生效 tag(整 key 参与哈希)。全程按 bytes,不假设 UTF-8。
///
/// # 参数
/// - `key`: Redis key 的原始字节。
pub fn effective_tag(key: &[u8]) -> Option<&[u8]> {
    let start = key.iter().position(|&b| b == b'{')?;
    let end_rel = key[start + 1..].iter().position(|&b| b == b'}')?;
    if end_rel == 0 {
        return None; // 空 {}:整 key 哈希,不找后续 tag(Redis 规则)
    }
    Some(&key[start + 1..start + 1 + end_rel])
}

/// 计算 key 的 Cluster slot(0..16384),与 `CLUSTER KEYSLOT` 一致。
///
/// # 参数
/// - `key`: Redis key 的原始字节。
pub fn redis_slot(key: &[u8]) -> u16 {
    let hashed = effective_tag(key).unwrap_or(key);
    crc16(hashed) % 16384
}

/// synthetic tag 表:slot → 一个保证落在该 slot 的短 tag(不含 '{' '}' ':')。
/// 固定生成算法 v1:从 0 起遍历计数器,十六进制小写编码为 "t{hex}",首个命中该 slot 的胜出。
/// 表内容由算法唯一决定,跨进程/跨版本稳定(算法变更 = key layout 升版)。
fn synthetic_table() -> &'static Vec<String> {
    static TABLE: OnceLock<Vec<String>> = OnceLock::new();
    TABLE.get_or_init(|| {
        let mut table: Vec<Option<String>> = vec![None; 16384];
        let mut remaining = 16384usize;
        let mut counter: u64 = 0;
        while remaining > 0 {
            let tag = format!("t{counter:x}");
            let slot = (crc16(tag.as_bytes()) % 16384) as usize;
            if table[slot].is_none() {
                table[slot] = Some(tag);
                remaining -= 1;
            }
            counter += 1;
        }
        table
            .into_iter()
            .map(|t| t.expect("synthetic 表全覆盖"))
            .collect()
    })
}

/// 取目标 slot 的 synthetic tag(构造期已保证 CRC16(tag)%16384 == slot)。
///
///**当前 partition cluster 同槽走 ④B 的 group 级 `{group}` relayout(整组同 slot),
/// 不经此函数**。`synthetic_tag`/`derive_same_slot_key` 是 **per-key 细粒度同槽派生**的预留能力,
/// 留给后续 **PollDomain / pipeline 按 slot 分桶**(把不同分区散到不同 slot 同时保证每组 Lua 的 key 同槽)。
/// 保留为 pub API(非死代码，后续按 slot 分布会用);现阶段 group 级 relayout 不调用。
///
/// # 参数
/// - `slot`: 目标 Redis Cluster slot,超出范围时按 16384 取模。
pub fn synthetic_tag(slot: u16) -> &'static str {
    &synthetic_table()[(slot % 16384) as usize]
}

/// 为 source key 派生同 slot 的辅助 key:`{<tag>}:<suffix>`(per-key 同槽派生;预留,见 `synthetic_tag` 注)。
/// source 有生效 tag → 复用;无 → 按 redis_slot(source) 取 synthetic tag。
///
/// # 参数
/// - `source`: 源 Redis key 的原始字节。
/// - `suffix`: 派生辅助 key 的后缀段。
pub fn derive_same_slot_key(source: &[u8], suffix: &str) -> Vec<u8> {
    let tag: &[u8] = match effective_tag(source) {
        Some(t) => t,
        None => synthetic_tag(redis_slot(source)).as_bytes(),
    };
    let mut out = Vec::with_capacity(tag.len() + suffix.len() + 3);
    out.push(b'{');
    out.extend_from_slice(tag);
    out.push(b'}');
    out.push(b':');
    out.extend_from_slice(suffix.as_bytes());
    out
}
