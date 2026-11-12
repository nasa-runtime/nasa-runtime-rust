use nacos_macro::response;

const CONFIG_NOT_FOUND: i32 = 300;
const CONFIG_QUERY_CONFLICT: i32 = 400;

/// 服务端配置读取结果，正文与元数据是否完整仍由读取流程检查。
#[response(identity = "ConfigQueryResponse", module = "config")]
pub(crate) struct ConfigQueryResponse {
    /// 正文格式，如 json、properties、txt 或 xml；不在此处执行对应解析。
    pub(crate) content_type: Option<String>,
    /// 配置正文，可能需要配置过滤器进行解密或其它处理。
    pub(crate) content: Option<String>,
    /// 服务端提供的内容 MD5，供变化比较使用。
    pub(crate) md5: Option<String>,
    /// 加密正文关联的数据密钥材料，由加密过滤器解释。
    pub(crate) encrypted_data_key: Option<String>,

    /// 服务端附带的配置标签，当前读取流程不据此过滤配置。
    pub(crate) tag: Option<String>,
    /// 服务端提供的最后修改时间戳，缓存原样保留，不以本地接收时间覆盖。
    pub(crate) last_modified: i64,
    /// 服务端是否将该内容标记为灰度配置。
    pub(crate) beta: bool,
}

impl ConfigQueryResponse {
    /// 业务作用：识别服务端配置不存在的错误类别，供读取流程返回对应失败语义。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：error_code 为 300 时为真；此方法不同时检查 result_code 或正文是否为空。
    pub(crate) fn is_not_found(&self) -> bool {
        self.error_code == CONFIG_NOT_FOUND
    }
    /// 业务作用：识别配置查询冲突，供上层区分临时冲突与其它读取失败。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：error_code 为 400 时为真；不自动等待、重试或改变响应状态。
    pub(crate) fn is_query_conflict(&self) -> bool {
        self.error_code == CONFIG_QUERY_CONFLICT
    }
}
