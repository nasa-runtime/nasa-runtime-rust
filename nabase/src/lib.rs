//! base —— 公共 API 与基础工具库。
//!
//! 本 crate 聚合响应结构、日期时间、容量、ID、字符串、环境变量和翻译抽象。所有模块都不持有
//! 外部系统连接或应用生命周期；业务通过 `nasa` 的 `base` feature 一次获得完整能力集合。
#![forbid(unsafe_code)]

pub mod date;
pub mod env;
pub mod id;
mod response;
pub mod size;
pub mod strings;
pub mod translator;

pub use id::{IdGenerate, Snowflake, SnowflakeConfig, SnowflakeError};
pub use response::BaseResponse;
pub use size::{ByteSize, ByteSizeError};
