//! 分层配置加载器与本地配置变化观察组件。
//!
//! 负责从本地配置文件、profile overlay、内存 overlay 和环境变量加载配置，
//! 并在反序列化前完成占位符解析与 import 声明解析。tracked 加载入口同时返回
//! 本轮实际来源，调用方可用可选 `watch` feature 精确观察主文件、profile 与附加依赖。
//!
//! watcher 只报告来源变化，不持有活动业务配置，也不替应用执行候选校验、资源准备、
//! 配置发布或回滚。应用收到事件后重新调用同一个 [`YmlLoader`]，并自行决定是否发布结果。
//!
//! 来源优先级固定为主配置、profile、内存 overlay、环境变量；后加载的来源覆盖先加载来源。
//!
//! # 加载架构
//!
//! `YmlLoader` 先确定主文件与活动 profile，再按固定优先级做叶子级深合并，随后解析占位符并
//! 反序列化。tracked 入口把配置与本轮来源一起返回，使加载、指纹和文件观察共享同一来源事实。
//!
//! # 文件观察边界
//!
//! `watch` feature 提供精确父目录观察和来源分类，但不执行去抖、候选校验或业务配置发布。
//! 动态对账先观察全部新增目录，再发布新目标，最后撤销旧目录；新增目录不可观察时保留旧目标，
//! 避免调用方进入只有部分来源可见的状态。
#![forbid(unsafe_code)]

mod imports;
mod loader;
mod placeholder;
mod source;

#[cfg(feature = "watch")]
pub mod watch;

pub use imports::{parse_imports_from_tree, NacosImport, YmlImport};
pub use loader::{ConfigFormat, LoadedYml, YmlLoader, YmlOverlay};
pub use source::YmlLocalSources;

// 分阶段加载方可先解析本地 bootstrap 树中的占位符，再从确定值中提取 import 声明。
pub use placeholder::resolve_placeholders;
