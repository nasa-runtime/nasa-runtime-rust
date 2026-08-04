//! 单请求大小、时间窗和全进程临时内存限制。

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::timeout;

use crate::MappingBuildError;

use super::{CryptoError, CryptoErrorKind};

/// 密码运行时所有请求、等待和时钟窗口的统一硬上限。
pub const MAX_CRYPTO_RUNTIME_DURATION: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// 端点密码处理的公开资源上限。
#[derive(Debug, Clone)]
pub struct CryptoLimits {
    /// 请求外层 JSON 信封最大字节数，读取 body 时再次硬截断。
    pub max_request_envelope_bytes: usize,
    /// 响应外层 JSON 信封最大字节数。
    pub max_response_envelope_bytes: usize,
    /// Base64 解码后的请求密文最大字节数。
    pub max_ciphertext_bytes: usize,
    /// 请求解密结果最大字节数。
    pub max_plaintext_bytes: usize,
    /// handler 返回且尚未加密的响应最大字节数。
    pub max_response_plaintext_bytes: usize,
    /// 响应加密后的二进制密文最大字节数。
    pub max_response_ciphertext_bytes: usize,
    /// 业务 JSON 允许的最大嵌套深度，根节点深度为一。
    pub max_json_depth: usize,
    /// 单个信封字符串字段允许的最大 UTF-8 字节数。
    pub max_string_bytes: usize,
    /// 单个严格信封允许的最大字段数量。
    pub max_fields: usize,
    /// 从进入 endpoint composer 到完成响应的总时间上限。
    pub max_request_duration: Duration,
    /// 客户端时间戳最多可落后服务端的时间。
    pub clock_past_window: Duration,
    /// 客户端时间戳最多可领先服务端的时间。
    pub clock_future_skew: Duration,
}

impl Default for CryptoLimits {
    /// 建立适合普通 JSON API 的保守资源上限。
    ///
    /// # 返回
    ///
    /// 返回适合普通 JSON API 的默认限制，应用仍应按业务负载显式配置。
    fn default() -> Self {
        Self {
            max_request_envelope_bytes: 1_100_000,
            max_response_envelope_bytes: 2_097_152,
            max_ciphertext_bytes: 786_432,
            max_plaintext_bytes: 524_288,
            max_response_plaintext_bytes: 1_048_576,
            max_response_ciphertext_bytes: 1_572_864,
            max_json_depth: 64,
            max_string_bytes: 1_100_000,
            max_fields: 7,
            max_request_duration: Duration::from_secs(5),
            clock_past_window: Duration::from_secs(60),
            clock_future_skew: Duration::from_secs(5),
        }
    }
}

impl CryptoLimits {
    /// 校验上限之间不会形成明显的无界或自相矛盾配置。
    ///
    /// # 返回
    ///
    /// 配置可用时返回空值；零值、异常 JSON 深度或密文上限倒置会拒绝启动。
    pub fn validate(&self) -> Result<(), MappingBuildError> {
        let non_zero = self.max_request_envelope_bytes > 0
            && self.max_response_envelope_bytes > 0
            && self.max_ciphertext_bytes > 0
            && self.max_plaintext_bytes > 0
            && self.max_response_plaintext_bytes > 0
            && self.max_response_ciphertext_bytes > 0
            && self.max_string_bytes > 0
            && self.max_fields > 0
            && !self.max_request_duration.is_zero();
        if !non_zero
            || self.max_request_duration > MAX_CRYPTO_RUNTIME_DURATION
            || self.clock_past_window > MAX_CRYPTO_RUNTIME_DURATION
            || self.clock_future_skew > MAX_CRYPTO_RUNTIME_DURATION
            || self.max_json_depth == 0
            || self.max_json_depth > 128
            || self.max_fields < 7
        {
            return Err(MappingBuildError::new(
                "crypto limits 含零值、超长时限、非法 JSON 深度或不足七个信封字段",
            ));
        }
        if self.max_request_envelope_bytes < self.max_ciphertext_bytes
            || self.max_response_envelope_bytes < self.max_response_ciphertext_bytes
        {
            return Err(MappingBuildError::new(
                "crypto envelope 上限小于二进制密文上限",
            ));
        }
        Ok(())
    }

    /// 计算请求解密阶段必须在读取 body 前预留的最坏临时字节数。
    ///
    /// 外层 HTTP body、JSON 信封中的 Base64 文本、解码密文和解密明文可能同时存在，因此不能
    /// 等读取完成后再按实际长度申请，否则大量并发请求可以在 admission 前突破进程预算。
    ///
    /// # 返回
    ///
    /// 返回使用饱和加法计算的最坏并存字节数；溢出会得到 `usize::MAX`，随后由预算校验拒绝。
    pub fn request_memory_reservation(&self) -> usize {
        self.max_request_envelope_bytes
            .saturating_mul(2)
            .saturating_add(self.max_ciphertext_bytes)
            .saturating_add(self.max_plaintext_bytes)
    }

    /// 计算响应重写阶段在收集 handler body 前必须预留的最坏临时字节数。
    ///
    /// handler body、可清零的拥有明文、provider 密文和最终信封在响应改写期间可能重叠；提前按
    /// 上限计费可防止并发大响应绕过同一全进程预算。
    ///
    /// # 返回
    ///
    /// 返回使用饱和加法计算的最坏并存字节数；溢出由预算容量检查收敛为启动错误。
    pub fn response_memory_reservation(&self) -> usize {
        self.max_response_plaintext_bytes
            .saturating_mul(2)
            .saturating_add(self.max_response_ciphertext_bytes)
            .saturating_add(self.max_response_envelope_bytes)
    }
}

/// 全进程密码临时缓冲区的加权内存预算。
#[derive(Clone)]
pub struct CryptoMemoryBudget {
    semaphore: Arc<Semaphore>,
    total_bytes: usize,
    acquire_timeout: Duration,
}

impl std::fmt::Debug for CryptoMemoryBudget {
    /// 输出容量和当前可用量，不输出任何缓冲区内容。
    ///
    /// # 参数
    ///
    /// - `formatter`: 调试输出目标。
    ///
    /// # 返回
    ///
    /// 返回标准格式化结果。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CryptoMemoryBudget")
            .field("total_bytes", &self.total_bytes)
            .field("available_bytes", &self.semaphore.available_permits())
            .finish_non_exhaustive()
    }
}

impl CryptoMemoryBudget {
    /// 建立全进程加权内存预算。
    ///
    /// # 参数
    ///
    /// - `total_bytes`: 所有并发密码请求可共同持有的临时字节数，范围为 1 到 `u32::MAX`。
    /// - `acquire_timeout`: 等待预算的最长时间，超时后快速拒绝请求。
    ///
    /// # 返回
    ///
    /// 配置合法时返回预算；范围不合法会拒绝启动。
    pub fn new(total_bytes: usize, acquire_timeout: Duration) -> Result<Self, MappingBuildError> {
        if total_bytes == 0
            || total_bytes > u32::MAX as usize
            || total_bytes > Semaphore::MAX_PERMITS
            || acquire_timeout.is_zero()
            || acquire_timeout > MAX_CRYPTO_RUNTIME_DURATION
        {
            return Err(MappingBuildError::new("crypto memory budget 配置非法"));
        }
        Ok(Self {
            semaphore: Arc::new(Semaphore::new(total_bytes)),
            total_bytes,
            acquire_timeout,
        })
    }

    /// 验证进程预算至少容纳一条最大请求或最大响应的临时缓冲区。
    ///
    /// # 参数
    ///
    /// - `limits`: 与该预算放入同一密码运行时快照的单请求限制。
    ///
    /// # 返回
    ///
    /// 两个方向的最坏申请都不超过总容量时返回空值，否则拒绝启动，避免服务启动后所有最大合法
    /// 请求都稳定失败。
    pub fn validate_limits(&self, limits: &CryptoLimits) -> Result<(), MappingBuildError> {
        let required = limits
            .request_memory_reservation()
            .max(limits.response_memory_reservation());
        if required > self.total_bytes || required > u32::MAX as usize {
            Err(MappingBuildError::new(
                "crypto memory budget 小于单请求最坏缓冲区需求",
            ))
        } else {
            Ok(())
        }
    }

    /// 为一次请求按最坏并存缓冲区申请预算。
    ///
    /// # 参数
    ///
    /// - `bytes`: 本次阶段可能同时持有的最大字节数，不得超过进程总预算。
    ///
    /// # 返回
    ///
    /// 成功返回 RAII permit；等待超时或容量不足返回 overload，始终 fail closed。
    pub async fn reserve(&self, bytes: usize) -> Result<OwnedSemaphorePermit, CryptoError> {
        if bytes == 0 || bytes > self.total_bytes || bytes > u32::MAX as usize {
            return Err(CryptoError::new(
                CryptoErrorKind::Overloaded,
                "memory-reservation-out-of-range",
            ));
        }
        timeout(
            self.acquire_timeout,
            self.semaphore.clone().acquire_many_owned(bytes as u32),
        )
        .await
        .map_err(|_| CryptoError::new(CryptoErrorKind::Overloaded, "memory-acquire-timeout"))?
        .map_err(|_| CryptoError::new(CryptoErrorKind::Internal, "memory-budget-closed"))
    }
}
