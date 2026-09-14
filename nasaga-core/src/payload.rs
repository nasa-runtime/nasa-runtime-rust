//! 业务正文的原始字节与不可变 schema 合同。

use serde::{Deserialize, Serialize};

/// 业务作用：把媒体类型与 schema 身份绑定到步骤定义，禁止端点轮换改变正文解释规则。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SagaPayloadContract {
    /// 小写规范媒体类型，不包含传输参数。
    pub content_type: String,
    /// 业务 schema 的稳定身份；无 schema 的 JSON 使用空字符串。
    pub schema_id: String,
}

impl Default for SagaPayloadContract {
    /// 业务作用：保留无 schema JSON 步骤的既有业务合同。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：application/json 与空 schema 身份。
    fn default() -> Self {
        Self {
            content_type: "application/json".to_owned(),
            schema_id: String::new(),
        }
    }
}

impl SagaPayloadContract {
    /// 业务作用：验证媒体类型与 schema 的规范形式，避免签名或摘要接受等价别名。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：字段规范且二进制正文具有 schema 身份时成功，否则返回稳定拒绝。
    pub fn validate(&self) -> Result<(), SagaPayloadError> {
        let Some((kind, subtype)) = self.content_type.split_once('/') else {
            return Err(SagaPayloadError);
        };
        if self.content_type.len() > 128
            || kind.is_empty()
            || subtype.is_empty()
            || !kind
                .bytes()
                .chain(subtype.bytes())
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"!#$&^_.+-".contains(&b))
            || self.schema_id.len() > 256
            || self.schema_id.trim() != self.schema_id
            || self.schema_id.chars().any(char::is_control)
            || (self.content_type != "application/json" && self.schema_id.is_empty())
        {
            return Err(SagaPayloadError);
        }
        Ok(())
    }
}

/// 业务作用：在 API、Outbox 与参与方之间保留正文的精确字节和 schema 身份。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SagaPayload {
    /// 正文的规范媒体类型。
    pub content_type: String,
    /// 与冻结步骤一致的 schema 身份。
    pub schema_id: String,
    /// 原始业务字节；JSON 空白、整数文本和对象字段顺序均保持不变。
    pub body: Vec<u8>,
}

impl SagaPayload {
    /// 业务作用：构造可进入摘要和持久消息的原始正文，不重新编码业务内容。
    ///
    /// 参数说明：content_type 与 schema_id 声明解释合同，body 是原始字节。
    ///
    /// 返回：合同合法时返回正文；非法媒体类型、schema 或 JSON 返回错误。
    pub fn new(
        content_type: impl Into<String>,
        schema_id: impl Into<String>,
        body: Vec<u8>,
    ) -> Result<Self, SagaPayloadError> {
        let payload = Self {
            content_type: content_type.into(),
            schema_id: schema_id.into(),
            body,
        };
        payload.validate()?;
        Ok(payload)
    }

    /// 业务作用：在进入摘要、业务解码或持久消息前复验公开正文结构。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：媒体与 schema 合法、JSON 可解析时成功；不改写原始正文。
    pub fn validate(&self) -> Result<(), SagaPayloadError> {
        self.contract().validate()?;
        if self.content_type == "application/json" {
            serde_json::from_slice::<serde_json::Value>(&self.body)
                .map_err(|_| SagaPayloadError)?;
        }
        Ok(())
    }

    /// 业务作用：提取定义与 capability 必须共同确认的正文解释合同。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：不包含业务字节的媒体类型和 schema 身份。
    pub fn contract(&self) -> SagaPayloadContract {
        SagaPayloadContract {
            content_type: self.content_type.clone(),
            schema_id: self.schema_id.clone(),
        }
    }

    /// 业务作用：把已有 JSON 值转换为无 schema 的规范 JSON 字节正文。
    ///
    /// 参数说明：value 是调用方已经解析的 JSON 值。
    ///
    /// 返回：无损整数序列化的 JSON 正文；需要保留输入文本时使用 new。
    pub fn json(value: serde_json::Value) -> Self {
        Self {
            content_type: "application/json".to_owned(),
            schema_id: String::new(),
            body: serde_json::to_vec(&value).expect("JSON value serialization"),
        }
    }
}

/// 正文解释合同或编码不合法的稳定拒绝，不包含业务正文。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SagaPayloadError;

impl std::fmt::Display for SagaPayloadError {
    /// 业务作用：输出可安全返回给调用者的正文合同拒绝。
    ///
    /// 参数说明：formatter 是文本输出目标。
    ///
    /// 返回：写入固定错误文本，不包含正文或 schema 内容。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Saga payload contract is invalid")
    }
}

impl std::error::Error for SagaPayloadError {}
