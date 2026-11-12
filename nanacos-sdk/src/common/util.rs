use crate::api::error::Error;
use crate::api::error::Result;
use std::sync::atomic::{AtomicU64, Ordering};

/// 首次读取时确定的本地地址，探测失败回退环回地址，不作为外部可达性证明。
pub(crate) static LOCAL_IP: std::sync::LazyLock<String> =
    std::sync::LazyLock::new(|| local_ipaddress::get().unwrap_or(String::from("127.0.0.1")));

/// 当前用户目录的兼容来源；缺失或无法编码时依次使用系统临时目录和固定临时路径。
#[allow(dead_code)]
pub(crate) static HOME_DIR: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    std::env::home_dir()
        .unwrap_or(std::env::temp_dir())
        .to_str()
        .map(|s| s.to_owned())
        .unwrap_or_else(|| "/tmp".to_owned())
});

/// 业务作用：为模块客户端分配含本地序号的标识，区分同一配置下的多个会话对象。
///
/// 参数说明：
/// - `module_name`：客户端所属模块名称，作为标识前缀。
/// - `server_addr`：配置的服务端地址串，不在此处规范化或校验。
/// - `namespace`：客户端命名空间标识，保留原文。
/// - `seq`：调用方共享的原子序号，读取原值后递增。
///
/// 返回：以 -_- 分隔模块、地址、命名空间和序号的文本；只提供该序号域内的区分，不承诺跨进程或计数回绕后的唯一性。
pub(crate) fn generate_client_id(
    module_name: &str,
    server_addr: &str,
    namespace: &str,
    seq: &AtomicU64,
) -> String {
    format!(
        "{module_name}-_-{server_addr}-_-{namespace}-_-{}",
        seq.fetch_add(1, Ordering::SeqCst)
    )
}

/// 业务作用：为未指定分组的请求补入 Nacos 默认分组，统一服务发现目标定位。
///
/// 参数说明：
/// - `group_name`：调用方给出的可选分组名，只有缺失或空字符串才采用默认值。
///
/// 返回：非空原值或 DEFAULT_GROUP；不裁剪空白，纯空白名称不会在此处被替换。
pub(crate) fn normalize_group_name(group_name: Option<String>) -> String {
    group_name
        .filter(|data| !data.is_empty())
        .unwrap_or_else(|| crate::api::constants::DEFAULT_GROUP.to_owned())
}

/// 业务作用：比较无序快照是否包含相同元素及重复次数，避免仅顺序变化触发更新。
///
/// 参数说明：
/// - `a`：待比较的一侧快照，不改变其元素顺序。
/// - `b`：另一侧快照，长度不同可直接判定不等。
///
/// 返回：排序后的引用列表相等时为真；重复元素数量参与比较，不将输入当作去重集合。
pub(crate) fn unordered_eq<T>(a: &[T], b: &[T]) -> bool
where
    T: Ord,
{
    if a.len() != b.len() {
        return false;
    }
    let mut a_sorted: Vec<&T> = a.iter().collect();
    let mut b_sorted: Vec<&T> = b.iter().collect();
    a_sorted.sort();
    b_sorted.sort();
    a_sorted == b_sorted
}

/// 业务作用：在发送请求前拒绝缺少实际内容的必填文本参数。
///
/// 参数说明：
/// - `param_val`：待校验的文本，使用裁剪后的结果判断是否为空，但不更改传入内容。
/// - `param_name`：用于错误定位的参数名，不应携带参数值或秘密材料。
///
/// 返回：非空白时返回原始借用；空白时返回 InvalidParam，错误仅携带名称和固定原因。
pub(crate) fn check_not_blank<'a>(param_val: &'a str, param_name: &'a str) -> Result<&'a str> {
    if param_val.trim().is_empty() {
        Err(Error::InvalidParam(
            param_name.into(),
            "param must not blank!".into(),
        ))
    } else {
        Ok(param_val)
    }
}
