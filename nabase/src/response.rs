//! 统一响应结构。

use serde::{Deserialize, Serialize};

/// 统一响应壳。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaseResponse<T> {
    /// 业务状态码:200 成功,非 200 失败。
    pub code: i32,
    /// 提示信息;`None` 时序列化省略。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub msg: Option<String>,
    /// 需要加密时返回的 AES 密钥;`None` 时序列化省略。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aes: Option<String>,
    /// 具体数据;`None` 时序列化省略。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
}

impl<T> BaseResponse<T> {
    /// 业务作用：构造成功响应。
    ///
    /// # 参数
    ///
    /// - `data`: 成功时返回给调用方的业务数据，会写入响应体的 `data` 字段。
    pub fn ok(data: T) -> Self {
        Self {
            code: 200,
            msg: None,
            aes: None,
            data: Some(data),
        }
    }

    /// 业务作用：构造失败响应。
    ///
    /// # 参数
    ///
    /// - `code`: 业务失败码；非 `200` 表示失败，调用方可据此分支处理。
    /// - `msg`: 失败提示文案，会写入响应体的 `msg` 字段。
    pub fn err(code: i32, msg: impl Into<String>) -> Self {
        Self {
            code,
            msg: Some(msg.into()),
            aes: None,
            data: None,
        }
    }

    /// 业务作用：设置响应加密密钥。
    ///
    /// # 参数
    ///
    /// - `aes`: 本次响应关联的 AES 密钥或密钥标识，会写入响应体的 `aes` 字段。
    pub fn with_aes(mut self, aes: impl Into<String>) -> Self {
        self.aes = Some(aes.into());
        self
    }
}
