use crate::common::remote::generate_request_id;
use nacos_macro::request;
use std::collections::HashMap;

/// 客户端提交配置正文及发布条件的协议请求。
#[request(identity = "ConfigPublishRequest", module = "config")]
pub(crate) struct ConfigPublishRequest {
    /// 待发布正文，不在协议对象内执行加解密。
    pub(crate) content: String,
    /// 条件写入要求匹配的旧内容 MD5，省略时不要求该条件。
    pub(crate) cas_md5: Option<String>,
    /// 内容类型、灰度范围和加密元数据等扩展字段。
    pub(crate) addition_map: HashMap<String, String>,
}

impl ConfigPublishRequest {
    /// 业务作用：构造一次配置发布请求，明确配置作用域、正文和请求关联标识。
    ///
    /// 参数说明：
    /// - `data_id`：待发布配置的标识。
    /// - `group`：配置分组。
    /// - `namespace`：配置所属命名空间。
    /// - `content`：待提交的配置正文，可能已由过滤器处理。
    ///
    /// 返回：未设置 CAS 条件及附加参数的请求；不发送、校验权限或更新缓存。
    pub fn new(data_id: String, group: String, namespace: String, content: String) -> Self {
        Self {
            request_id: Some(generate_request_id()),
            data_id: Some(data_id),
            group: Some(group),
            namespace: Some(namespace),
            content,
            cas_md5: None,
            addition_map: HashMap::default(),
            ..Default::default()
        }
    }
    /// 业务作用：为配置发布指定旧内容摘要，交由服务端执行条件写入。
    ///
    /// 参数说明：
    /// - `cas_md5`：要求匹配的旧内容 MD5；为空时不附加该条件。
    ///
    /// 返回：保存条件的请求，不在本地计算摘要或判断条件是否满足。
    pub fn cas_md5(mut self, cas_md5: Option<String>) -> Self {
        self.cas_md5 = cas_md5;
        self
    }

    /// 业务作用：为配置发布附加一个协议扩展参数，如内容类型或加密元数据。
    ///
    /// 参数说明：
    /// - `key`：扩展参数键名。
    /// - `val`：参数值，覆盖同名字段。
    ///
    /// 返回：原地更新附加参数，不改变正文或独立 CAS 字段，也不发送请求。
    pub fn add_addition_param(&mut self, key: impl Into<String>, val: impl Into<String>) {
        self.addition_map.insert(key.into(), val.into());
    }

    /// 业务作用：合并业务提供的发布扩展参数，保留其它既有字段。
    ///
    /// 参数说明：
    /// - `addition_params`：移入请求的扩展键值，同名键以本次值为准。
    ///
    /// 返回：原地更新附加参数集合，不执行字段合法性校验或远端发布。
    pub fn add_addition_params(&mut self, addition_params: HashMap<String, String>) {
        self.addition_map.extend(addition_params);
    }
}
