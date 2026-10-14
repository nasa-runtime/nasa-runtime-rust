//! 底层客户端隔离层；任何外部客户端类型都不得穿透到公共 API。

pub(crate) mod admin;
// 配置查询和增量修改需要读取原生逐资源确认；指针所有权仅存在于该私有模块的同一阻塞任务内。
#[allow(unsafe_code)]
mod admin_mutation;
pub(crate) mod capabilities;
pub(crate) mod config;
pub(crate) mod consumer;
pub(crate) mod errors;
pub(crate) mod offset_observation;
pub(crate) mod producer;
pub(crate) mod transaction;
