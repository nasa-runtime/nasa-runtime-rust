//! 广播位点归属的共同编码，不依赖实现语言或 handler 路径。

use crate::{NafkaError, Result};
use sha2::{Digest, Sha256};

/// 业务作用：根据明确业务作用域和实例身份派生稳定广播组。
/// 参数说明：scope 为业务作用域，instance_id 为实例身份；均为非空 UTF-8、最多 4096 字节，不含控制字符及首尾 ASCII 空格。
/// 返回：最多 255 ASCII 字节的 group；短值无损编码，长值使用带长度边界的 SHA-256 摘要，非法声明返回 Config。
pub fn broadcast_group(scope: &str, instance_id: &str) -> Result<String> {
    let scope = component(scope)?;
    let instance = component(instance_id)?;
    let encoded = format!("nasa-kafka-broadcast-b{}.{}", hex(scope), hex(instance));
    if encoded.len() <= 255 {
        return Ok(encoded);
    }
    let mut digest = Sha256::new();
    digest.update(b"nasa.kafka.broadcast\0");
    // 分量长度进入摘要，阻止不同作用域与实例边界产生相同拼接输入。
    digest.update((scope.len() as u32).to_be_bytes());
    digest.update(scope);
    digest.update((instance.len() as u32).to_be_bytes());
    digest.update(instance);
    Ok(format!("nasa-kafka-broadcast-h{}", hex(&digest.finalize())))
}

/// 业务作用：保留身份原文并拒绝无法安全派生的声明。
/// 参数说明：value 为作用域或实例身份。
/// 返回：精确字节，不执行裁剪、替换或 Unicode 归一化；非法值在连接前拒绝。
fn component(value: &str) -> Result<&[u8]> {
    if value.is_empty()
        || value.len() > 4096
        || value.starts_with(' ')
        || value.ends_with(' ')
        || value.chars().any(char::is_control)
    {
        return Err(NafkaError::Config(
            "广播身份必须非空、无控制字符及首尾 ASCII 空格，且最多 4096 UTF-8 字节".into(),
        ));
    }
    Ok(value.as_bytes())
}

/// 业务作用：无损编码身份字节到受限 ASCII 字符集合。
/// 参数说明：bytes 为有界字节序列。
/// 返回：小写十六进制字符串，不省略前导零。
fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push(DIGITS[(byte >> 4) as usize] as char);
        result.push(DIGITS[(byte & 15) as usize] as char);
    }
    result
}
