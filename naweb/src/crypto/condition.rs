//! 只读取可信元数据的密码方向 condition。

use std::future::Future;
use std::pin::Pin;

use axum::http::{Extensions, HeaderMap, HeaderName};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use zeroize::Zeroizing;

use crate::CryptoDirections;

use super::{CryptoError, CryptoErrorKind};

/// 可信入口在剥离公网同名 header 后写入 request extensions 的证明标记。
#[derive(Debug, Clone, Copy)]
pub struct TrustedIngress;

/// crypto condition 可读取的无 body 请求视图。
#[derive(Clone, Copy)]
pub struct CryptoConditionInput<'a> {
    /// 大写 HTTP 方法，由已匹配 route policy 提供。
    pub method: &'a str,
    /// 已匹配路由的稳定标识。
    pub route_id: &'a str,
    /// 应用接收的 URI path，不含 body。
    pub path: &'a str,
    /// 受限请求头只读视图；condition 不得保存其内容。
    pub headers: &'a HeaderMap,
    /// 可信入口和身份层写入的扩展只读视图。
    pub extensions: &'a Extensions,
}

/// crypto condition 对象安全异步返回值。
pub type CryptoConditionFuture<'a> =
    Pin<Box<dyn Future<Output = Result<CryptoDirections, CryptoError>> + Send + 'a>>;

/// 只能关闭静态声明方向的密码条件扩展点。
pub trait CryptoCondition: Send + Sync + 'static {
    /// 业务作用：返回注册表使用的稳定 condition ID。
    ///
    /// # 返回
    ///
    /// 返回非空、有限长度的 ASCII 标识。
    fn id(&self) -> &'static str;

    /// 业务作用：根据可信元数据收窄本请求的加解密方向。
    ///
    /// # 参数
    ///
    /// - `input`: 不含 body 的只读请求视图，生命周期只覆盖当前 await。
    /// - `declared`: route policy 静态声明的方向上限。
    ///
    /// # 返回
    ///
    /// 返回声明子集；实现错误或试图打开未声明方向时 endpoint composer fail closed。
    fn evaluate<'a>(
        &'a self,
        input: CryptoConditionInput<'a>,
        declared: CryptoDirections,
    ) -> CryptoConditionFuture<'a>;
}

/// 仅供受控迁移入口使用的 legacy disable header 条件。
pub struct LegacyDisableHeaderCondition {
    header_name: HeaderName,
    expected_digest: Zeroizing<[u8; 32]>,
}

impl std::fmt::Debug for LegacyDisableHeaderCondition {
    /// 业务作用：输出 header 名但不输出作为 secret 的匹配值。
    ///
    /// # 参数
    ///
    /// - `formatter`: 调试输出目标。
    ///
    /// # 返回
    ///
    /// 返回脱敏格式化结果。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LegacyDisableHeaderCondition")
            .field("header_name", &self.header_name)
            .finish_non_exhaustive()
    }
}

impl LegacyDisableHeaderCondition {
    /// 业务作用：建立需要可信入口证明的 legacy 旁路条件。
    ///
    /// # 参数
    ///
    /// - `header_name`: 公网入口必须先剥离、再由可信网关注入的 header 名。
    /// - `expected_value`: 作为 secret 的精确匹配字节，禁止日志、回显和公开配置明文。
    ///
    /// # 返回
    ///
    /// 值非空时返回只保存固定长度摘要的 condition；空值返回配置错误。原始匹配值不会在构造后
    /// 继续驻留于对象中，避免调试、崩溃转储或普通比较路径暴露旁路凭证。
    pub fn new(header_name: HeaderName, expected_value: Vec<u8>) -> Result<Self, CryptoError> {
        if expected_value.is_empty() || expected_value.len() > 256 {
            return Err(CryptoError::new(
                CryptoErrorKind::Policy,
                "legacy-disable-value-invalid",
            ));
        }
        let expected_value = Zeroizing::new(expected_value);
        let expected_digest: [u8; 32] = Sha256::digest(expected_value.as_slice()).into();
        Ok(Self {
            header_name,
            expected_digest: Zeroizing::new(expected_digest),
        })
    }
}

impl CryptoCondition for LegacyDisableHeaderCondition {
    /// 业务作用：返回架构固定的显式 condition ID。
    ///
    /// # 返回
    ///
    /// 返回 `legacy-disable-header`。
    fn id(&self) -> &'static str {
        "legacy-disable-header"
    }

    /// 业务作用：只有 secret 匹配且存在可信入口证明时关闭已声明方向。
    ///
    /// # 参数
    ///
    /// - `input`: 已由公网边界完成伪造 header 剥离的请求视图。
    /// - `declared`: route policy 已声明方向。
    ///
    /// # 返回
    ///
    /// header 缺失时保持声明；存在但不可信或不匹配时拒绝；双重验证成功时返回空方向。
    fn evaluate<'a>(
        &'a self,
        input: CryptoConditionInput<'a>,
        declared: CryptoDirections,
    ) -> CryptoConditionFuture<'a> {
        Box::pin(async move {
            let Some(value) = input.headers.get(&self.header_name) else {
                return Ok(declared);
            };
            let received_digest =
                Zeroizing::new(<[u8; 32]>::from(Sha256::digest(value.as_bytes())));
            let trusted = input.extensions.get::<TrustedIngress>().is_some();
            // 比较固定长度摘要，既不保留原始旁路 secret，也避免普通切片比较的前缀时序差异。
            let matched = bool::from(
                received_digest
                    .as_ref()
                    .ct_eq(self.expected_digest.as_ref()),
            );
            if !trusted || !matched {
                return Err(CryptoError::new(
                    CryptoErrorKind::Policy,
                    "legacy-disable-untrusted",
                ));
            }
            Ok(CryptoDirections {
                request: false,
                response: false,
            })
        })
    }
}
