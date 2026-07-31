use serde_json::Value;

use crate::{ApplicationPhase, ApplicationResult, ComponentId};

/// 保留配置根与其负责组件的固定映射。
///
/// 该表同时服务三处：未声明组件的配置段告警、热刷新前的内置组件段校验，以及“该组件相关配置
/// 是否变化”的重启判定。新增内置组件时必须同步更新本表，否则相关配置会被静默忽略。
const RESERVED_ROOTS: &[(&str, ComponentId)] = &[
    ("log", ComponentId::Log),
    ("nacos", ComponentId::NacosConfig),
    ("database", ComponentId::Db),
    ("datasources", ComponentId::Db),
    ("redis", ComponentId::Redis),
    ("telemetry", ComponentId::Telemetry),
    ("cache", ComponentId::Cache),
    ("kafka", ComponentId::Kafka),
    ("kafkas", ComponentId::Kafka),
    ("auth", ComponentId::Auth),
    ("server", ComponentId::Web),
    ("ws", ComponentId::Ws),
    ("rest_discovery", ComponentId::NacosDiscovery),
    ("scheduling", ComponentId::Scheduling),
];

/// 对存在配置段但未声明对应组件的情况输出一次脱敏告警。
///
/// 只输出配置段名与组件名，绝不输出段内的地址、用户名或密码；告警只描述框架组件不会消费该段，
/// 不能断言业务代码没有通过 [`crate::Application::config`] 手动读取同名配置。
///
/// # 参数
///
/// - `components`：属性入口按源码顺序声明的组件列表。
/// - `tree`：需要检查的完整配置树；调用点使用最终树，从而同时覆盖本地与远端 overlay 引入的段。
pub(crate) fn warn_undeclared_sections(components: &[ComponentId], tree: &Value) {
    for (root, owner) in RESERVED_ROOTS {
        if tree.get(root).is_none() || components.contains(owner) {
            continue;
        }
        tracing::warn!(
            "config section `{root}` is present but component `{owner}` is not declared; \
             the framework component will not apply it (business code may still read the raw config snapshot)"
        );
    }
}

/// 在发布候选配置前校验所有已声明内置组件的配置段。
///
/// 校验只做无副作用的反序列化：任一段非法时整帧候选都不发布，旧快照继续有效。
/// `application` 段不在此校验范围内——它是 bootstrap-only 的，运行期只做原始 section 比较。
///
/// # 参数
///
/// - `components`：属性入口声明的组件列表，决定哪些段属于“已声明”。
/// - `tree`：合并、插值完成但尚未对外发布的候选配置树。
/// - `phase`：本轮校验所属生命周期；启动首帧与运行期热刷新必须如实区分。
pub(crate) fn validate_declared_sections(
    components: &[ComponentId],
    tree: &Value,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    // 最小核心构建不含任何段校验器；显式借用让同一函数在该裁剪组合下仍保持无告警编译。
    let _ = (tree, phase);
    for component in components {
        match component {
            #[cfg(feature = "web")]
            ComponentId::Web => crate::web::validate_server_section(tree, phase)?,
            #[cfg(feature = "log")]
            ComponentId::Log => crate::log::validate_log_section(tree, phase)?,
            #[cfg(feature = "nacos-config")]
            ComponentId::NacosConfig => crate::nacos_config::validate_nacos_section(tree, phase)?,
            #[cfg(feature = "db")]
            ComponentId::Db => crate::db::validate_datasource_sections(tree, phase)?,
            #[cfg(feature = "redis")]
            ComponentId::Redis => crate::redis::validate_redis_section(tree, phase)?,
            #[cfg(feature = "telemetry")]
            ComponentId::Telemetry => crate::telemetry::validate_telemetry_section(tree, phase)?,
            #[cfg(feature = "cache")]
            ComponentId::Cache => crate::cache::validate_cache_section(tree, phase)?,
            #[cfg(feature = "kafka")]
            ComponentId::Kafka => crate::kafka::validate_kafka_sections(tree, phase)?,
            #[cfg(feature = "web")]
            ComponentId::Auth => crate::auth::validate_auth_section(tree, phase)?,
            #[cfg(feature = "scheduling")]
            ComponentId::Scheduling => crate::scheduling::validate_scheduling_section(tree, phase)?,
            #[cfg(feature = "nacos-discovery")]
            ComponentId::NacosDiscovery => {
                crate::discovery::validate_discovery_section(tree, phase)?
            }
            #[cfg(feature = "ws")]
            ComponentId::Ws => crate::ws::validate_ws_section(tree, phase)?,
            // 内部身份没有配置根；若手写 Runner 放入它们，组件顺序校验会在产生副作用前拒绝。
            _ => {}
        }
    }
    Ok(())
}

/// 判断某个组件负责的全部配置段在两棵树之间是否发生变化。
///
/// 用于区分“该组件相关配置未变，可把 applied_version 推进到新快照”与“配置已变但本版本无法热应用，
/// 必须报 RestartRequired”。比较的是原始子树，因此新增未知字段同样算变化。
///
/// # 参数
///
/// - `component`：需要判断的组件身份。
/// - `current`：当前已发布快照的配置树。
/// - `candidate`：尚未发布的候选配置树。
#[cfg(feature = "nacos-config")]
pub(crate) fn sections_changed(component: ComponentId, current: &Value, candidate: &Value) -> bool {
    RESERVED_ROOTS
        .iter()
        .filter(|(_, owner)| *owner == component)
        .any(|(root, _)| current.get(root) != candidate.get(root))
}
