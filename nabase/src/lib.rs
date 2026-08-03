//! base —— 公共 API 与轻量工具库。
//!
//! 默认能力只放无运行时、无外部系统、无业务生命周期的公共结构、纯工具和小型算法
//! （依赖只有 serde）。日期、数值、密码和图像等重能力保持为独立 crate，
//! 由 `nasa::{date,numeric,crypto,image}` 顶层 feature 暴露,不在 base 内重复聚合。
#![forbid(unsafe_code)]

pub mod env;
pub mod id;
mod response;
pub mod size;
pub mod strings;
pub mod translator;

pub use id::{IdGenerate, Snowflake, SnowflakeConfig, SnowflakeError};
pub use response::BaseResponse;
pub use size::{ByteSize, ByteSizeError};
