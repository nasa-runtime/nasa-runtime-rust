//! RestDiscovery 错误类型。
//!
//! 硬约束:错误必须能区分「未初始化 / discovery 禁用下的内部调用 /
//! 没有 provider / URL 非法 / 未知服务 host / 无可用实例 / 发现失败 / HTTP 发送失败 / 响应状态错误」,
//! **不许统一塞进 `anyhow!("request failed")`**。故这里用 typed enum。

use thiserror::Error;

/// crate 内统一 Result 别名。
pub type Result<T> = std::result::Result<T, RestDiscoveryError>;

/// RestDiscovery 调用链上所有可区分的失败。
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RestDiscoveryError {
    /// `RestDiscovery::get()/try_get()` 在 init 之前被调用。
    #[error("RestDiscovery 未初始化:请先在 main 调用 RestDiscovery::init_with_discovery 或 init_external_only")]
    NotInitialized,

    /// 重复 init(避免多模块启动悄悄覆盖全局实例)。
    #[error("RestDiscovery 已初始化:不能重复 init")]
    AlreadyInitialized,

    /// external-only(discovery 禁用)模式下做了显式内部调用(`service_request` / `lb://`)。
    /// **不退化成 `http://service` 的 DNS 请求**。
    #[error("discovery 已禁用(external-only),不能对内部服务 {service} 做显式调用;请检查 rest_discovery.enabled")]
    DiscoveryDisabledForInternalCall {
        /// 调用方试图访问的内部服务名。
        service: String,
    },

    /// 构造 client 时传入的 `RestDiscoveryOptions` 非法(如某时长字段为 0、`restart_backoff_min > max`)。
    /// direct API(`try_new`/`try_external_only`/`connect`)对此 fail-fast,而非靠后台 task panic / 静默 clamp
    /// 掩盖配置错误(配置门面的 yml 映射也做同等 fail-fast;clamp 仅作内部不变量兜底)。
    #[error("非法 RestDiscoveryOptions:{reason}")]
    InvalidOptions {
        /// 配置非法的可读原因。
        reason: String,
    },

    /// discovery 启用但未装配任何 provider。
    ///
    /// `init_with_discovery` 要求传入 provider，`init_external_only` 明确禁用 discovery；本分支作为
    /// “启用 discovery 但 provider 缺失或不受支持”的稳定错误边界，供配置门面直接映射。
    #[error("discovery 已启用但没有任何 provider")]
    NoDiscoveryProvider,

    /// URL / service / path 非法(非 http/https/lb scheme、service 含 `/?#`/空白、path 不以 `/` 开头或是完整 URL 等)。
    #[error("非法 URL {url}:{reason}")]
    InvalidUrl {
        /// 调用方传入的原始 URL 或服务路径。
        url: String,
        /// URL 不满足内部/外部调用规则的原因。
        reason: String,
    },

    /// 裸 http(s) 启发式模式下 host 不在服务名索引(仅 `UnknownHostPolicy::Error` 时返回;第三阶段才用)。
    #[error("未知服务 host {host}(不在服务名索引)")]
    UnknownServiceHost {
        /// 未命中服务名索引的 host。
        host: String,
    },

    /// 已确认是内部服务,但当前无可承载流量的实例(权威空列表)。**不 fallback DNS、不用 stale**。
    #[error("服务 {service} 无可用实例")]
    NoAvailableInstance {
        /// 当前没有可用实例的服务名。
        service: String,
    },

    /// 调用 provider 的 discover/watch 出错(网络/Nacos 短暂不可用等)。
    #[error("服务 {service} 发现失败:{source}")]
    DiscoveryFailed {
        /// 发现失败的服务名。
        service: String,
        #[source]
        /// 注册发现后端返回的原始错误。
        source: anyhow::Error,
    },

    /// 请求构造阶段失败(query/body 序列化等)。
    #[error("请求构造失败:{reason}")]
    RequestBuildFailed {
        /// 请求构造失败的原因,通常来自参数或 body 序列化。
        reason: String,
    },

    /// 调用链绝对 deadline 已耗尽；请求未发出或在到期时被取消。
    #[error("出站调用预算已耗尽")]
    BudgetExhausted,

    /// 父请求/调用方显式取消了预算。
    #[error("出站调用已取消")]
    Cancelled,

    /// 每服务 bulkhead 已达到并发上限；调用未进入发现或网络阶段。
    #[error("服务 {service} bulkhead 已满")]
    BulkheadRejected {
        /// 被隔离的内部服务名。
        service: String,
    },

    /// 服务熔断器仍处于打开状态，或已有 half-open 探针在途。
    #[error("服务 {service} 熔断器已打开")]
    CircuitOpen {
        /// 被熔断的内部服务名。
        service: String,
    },

    /// 动态服务名超过有界状态表容量。
    #[error("REST resilience 状态表已达硬上限")]
    ResilienceStateLimit,

    /// reqwest 发送/传输层错误。
    #[error("HTTP 发送失败:{0}")]
    Http(#[from] reqwest::Error),

    /// 便捷方法(send_json/send_text)遇到非 2xx;body 只保留有限摘要,避免日志打爆/泄露大响应。
    #[error("HTTP 状态错误 {status}:{body_snippet}")]
    HttpStatus {
        /// 下游返回的非 2xx HTTP 状态。
        status: reqwest::StatusCode,
        /// 响应体摘要,用于排障但避免记录完整大响应。
        body_snippet: String,
    },

    /// 2xx 响应体解码/解包失败(`unwrap = "data"`:body 非合法 JSON、缺解包字段、或字段值类型不匹配)。
    /// 与传输层 `Http` 区分:这是「连上了、状态也对,但响应内容不符合约定」的契约错误。
    #[error("响应解码失败:{reason}")]
    ResponseDecodeFailed {
        /// 响应体不符合调用方约定的原因。
        reason: String,
    },
}
