//! 分层配置加载器。
//!
//! 负责从本地配置文件、profile overlay、内存 overlay 和环境变量加载配置，
//! 并在反序列化前完成占位符解析与 import 声明解析。
// ============================================================================
// yml —— 通用分层 YAML 配置加载器(下沉自各 app 的 config.rs;对照)。
//
// 只认识【配置来源】与【反序列化目标类型 T】,不认识业务 AppConfig。来源优先级(低 → 高):
//   1. 本地主配置          zcf/application.yml
//   2. profile 叠加        zcf/application-{APP_PROFILE}.yml(**未设 APP_PROFILE 则不加载**,无默认;文件缺失不报错)
//   3. 内存 overlay        含 Nacos 拉回的多配置,由门面层按 import 顺序喂进来
//   4. 环境变量            APP__SECTION__KEY(最后叠加,最高优先级)
//   叠加完成后解析 ${a.b.c} 占位符,再反序列化成 T。
//
// 边界(对照):
//   不连接 Nacos、不保存全局、不做热替换、不定义 AppConfig、不初始化 日志/Redis/DB/服务发现。
//   import 解析只产出【中性】YmlImport(File / Nacos 描述,零 Nacos 依赖);
//   「按 imports 调 Nacos 拉取并拼装有序 overlay」的胶水放在 nasa 门面层(它同时依赖 yml 与 nacos)。
//
// 业务只经门面使用,不直接依赖本 crate:
//   use nasa::yml::{YmlLoader, YmlOverlay};
// ============================================================================
#![forbid(unsafe_code)]

mod imports;
mod loader;
mod placeholder;

pub use imports::{parse_imports_from_tree, NacosImport, YmlImport};
pub use loader::{ConfigFormat, YmlLoader, YmlOverlay};

// 占位符解析对外暴露:门面层在解析 import 前,需先对本地 bootstrap 树做一次 ${} 解析
// (才能把 `${server.name}.yml` 里的占位符解开、算出真正要拉的 dataId),再调 parse_imports_from_tree。
pub use placeholder::resolve_placeholders;
