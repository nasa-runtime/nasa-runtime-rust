//! 按 Redis 字节级 hash tag 规则计算 Cluster slot，并为任意源键派生同槽辅助键。
//!
//! 只采用第一个左括号与其后第一个右括号之间的非空内容；遇到空 tag 时使用完整键，
//! 不继续寻找后续 tag。没有有效 tag 时使用覆盖全部 slot 的确定性合成表。
//! 同槽不等于源键唯一，调用方必须在辅助键后缀中区分需要隔离的源。

use std::sync::OnceLock;

/// 业务作用：计算确定性的 FNV-1a 64 位摘要，供持久键或操作标识使用，避免进程随机种子改变命名。
///
/// 参数说明：
/// - `data`: 要参与稳定摘要计算的原始字节。
///
/// 返回：固定十六位小写十六进制文本；不是加密摘要，不提供无碰撞或抗伪造保证。
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

/// 业务作用：按 Redis Cluster 使用的 CRC16-XMODEM 算法计算路由校验值，多项式为 0x1021、初值为零。
///
/// 参数说明：
/// - `data`: 要计算 Redis slot CRC 的原始字节。
///
/// 返回：十六位 CRC 值；调用方按 16384 取模后才得到 slot。
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

/// 业务作用：Redis 有效 hash tag 提取:第一个 '{' 与其后第一个 '}' 之间**非空**才生效。
///
/// 参数说明：
/// - `key`: Redis key 的原始字节。
///
/// 返回：借用第一个有效 tag 的字节；首对括号为空、右括号缺失或没有左括号时返回 None，应对完整键哈希。
pub fn effective_tag(key: &[u8]) -> Option<&[u8]> {
    let start = key.iter().position(|&b| b == b'{')?;
    let end_rel = key[start + 1..].iter().position(|&b| b == b'}')?;
    // Redis 遇到首个空 tag 即按完整键路由，继续查找后续括号会让辅助键落入不同 slot。
    if end_rel == 0 {
        return None;
    }
    Some(&key[start + 1..start + 1 + end_rel])
}

/// 业务作用：按有效 hash tag 或完整键计算 Redis Cluster slot，供同槽约束及路由选择使用。
///
/// 参数说明：
/// - `key`: Redis key 的原始字节。
///
/// 返回：零到 16383 的稳定 slot 下标，不要求输入是 UTF-8。
pub fn redis_slot(key: &[u8]) -> u16 {
    let hashed = effective_tag(key).unwrap_or(key);
    crc16(hashed) % 16384
}

/// 业务作用：惰性构造覆盖全部 Cluster slot 的确定性短 tag 表，为没有有效 tag 的源键提供同槽坐标。
///
/// 参数说明：无。
///
/// 返回：全进程共享的完整表；计数器从零递增，以 `t` 加小写十六进制编码，保留每槽首个命中值。
/// 该选择规则决定持久键布局，改变算法需要迁移既有键。
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

/// 业务作用：取得目标 slot 的确定性合成 tag，为没有有效 tag 的源键构造同槽辅助键。
///
/// 参数说明：
/// - `slot`: 目标 Redis Cluster slot,超出范围时按 16384 取模。
///
/// 返回：全进程共享的短 tag，其 CRC16 按 16384 取模等于归一化后的目标 slot；首次调用构造完整表。
pub fn synthetic_tag(slot: u16) -> &'static str {
    &synthetic_table()[(slot % 16384) as usize]
}

/// 业务作用：为源键构造 `{tag}:suffix` 同槽辅助键，有效原 tag 直接复用，否则按源 slot 选择合成 tag。
///
/// 参数说明：
/// - `source`: 源 Redis key 的原始字节。
/// - `suffix`：辅助键后缀；需要隔离同槽源键时，调用方必须在此包含源身份。
///
/// 返回：新建的辅助键字节，与源键处于相同 slot；不自动附加源摘要，也不保证不同源键的输出唯一。
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
