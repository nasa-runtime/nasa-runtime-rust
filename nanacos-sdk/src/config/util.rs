/// 业务作用：为配置缓存构造无歧义的三字段身份，分隔符不会合并不同配置。
///
/// 参数说明：
/// - `data_id`：原始配置标识。
/// - `group`：原始分组。
/// - `namespace`：原始命名空间。
///
/// 返回：固定 JSON 数组，不执行路径解释或 Unicode 归一化。
pub(crate) fn group_key(data_id: &str, group: &str, namespace: &str) -> String {
    serde_json::to_string(&[data_id, group, namespace]).expect("string tuple serialization")
}
