//! 有界分层配置装配、嵌套表达式与来源观察。
//!
//! [`strict::ConfigLoader`] 把主文件、profile、有序文件名通配符导入和固定环境快照装配为
//! 完整候选，同时返回字段来源、表达式依赖、值指纹和目录观察计划。嵌套默认值支持
//! `${LOG_PATH:/usr/local/logs/${application.name}}`，环境覆盖及显式空值不会重复解析。
//!
//! [`YmlLoader`] 保留兼容入口的主文件、profile、内存 overlay 与环境优先级，不自动执行
//! `yml.imports`；严格来源通过显式入口启用。严格文本按目标类型绑定，字符串数字保持原文。
//! 文件名模式只支持 `*`、`?`；每组按完整文件名自然排序，后文件覆盖前文件，不跨声明重排。
//! `${aa.bb.cc}`、`${aa-bb-cc}`、`${AA_BB_CC}` 可回退到同一大写环境键，确切树值和原样环境名优先。
//! 兼容入口裁剪默认分支源码边缘空白，严格入口保留；嵌套引用已确定的类型不会重复推断。
//!
//! # 来源与发布边界
//!
//! 纯内存入口只使用调用方提供的文档与快照。远端文本由配置中心适配器提供，先解析来源
//! 引导依赖，最后在完整树上求值业务字段。naml 不连接配置中心或 secret provider。
//!
//! `watch` feature 观察实际文件、模式目录及可选缺失目录的祖先，只发重读信号，不调度
//! 去抖、业务校验、资源准备、运行态发布或跨组件回滚。宿主必须保留周期补读。
//! 新目录全部观察成功后才切换目标，失败保留旧观察；值相同也需对账来源集合。
#![forbid(unsafe_code)]

mod imports;
mod loader;
mod placeholder;
mod source;
pub mod strict;

#[cfg(feature = "watch")]
pub mod watch;

pub use imports::{parse_imports_from_tree, NacosImport, YmlImport};
pub use loader::{ConfigFormat, LoadedYml, YmlLoader, YmlOverlay};
pub use source::YmlLocalSources;

// 分阶段加载方可先解析本地 bootstrap 树中的占位符，再从确定值中提取 import 声明。
pub use placeholder::{resolve_placeholders, resolve_placeholders_preserving_unresolved};
