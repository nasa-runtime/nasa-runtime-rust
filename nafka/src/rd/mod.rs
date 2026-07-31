//! 底层客户端隔离层；任何外部客户端类型都不得穿透到公共 API。

pub(crate) mod admin;
pub(crate) mod config;
pub(crate) mod consumer;
pub(crate) mod producer;
