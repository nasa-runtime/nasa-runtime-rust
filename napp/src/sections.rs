#[cfg(any(
    feature = "outbox",
    feature = "saga",
    feature = "cache",
    feature = "scheduling"
))]
use std::collections::BTreeSet;

use serde_json::Value;

use crate::{ApplicationPhase, ApplicationResult, ComponentId};

/// 保留配置路径与其负责组件的固定映射。
///
/// 该表同时服务三处：未声明组件的配置段告警、热刷新前的内置组件段校验，以及“该组件相关配置
/// 是否变化”的重启判定。新增内置组件时必须同步更新本表，否则相关配置会被静默忽略。
const RESERVED_SECTIONS: &[(&str, ComponentId)] = &[
    ("log", ComponentId::Log),
    ("nacos", ComponentId::NacosConfig),
    ("database", ComponentId::Db),
    ("datasources", ComponentId::Db),
    ("redis", ComponentId::Redis),
    ("redis.job", ComponentId::RedisJob),
    ("telemetry", ComponentId::Telemetry),
    ("grpc", ComponentId::Grpc),
    ("cache", ComponentId::Cache),
    ("partition", ComponentId::Partition),
    ("saga", ComponentId::Saga),
    ("outbox", ComponentId::Outbox),
    ("kafka", ComponentId::Kafka),
    ("kafkas", ComponentId::Kafka),
    ("auth", ComponentId::Auth),
    ("server", ComponentId::Web),
    ("ws", ComponentId::Ws),
    ("rest_discovery", ComponentId::NacosDiscovery),
    ("scheduling", ComponentId::Scheduling),
];

/// 业务作用：对存在配置段但未声明对应组件的情况输出一次脱敏告警。
///
/// 只输出配置段名与组件名，绝不输出段内的地址、用户名或密码；告警只描述框架组件不会消费该段，
/// 不能断言业务代码没有通过 [`crate::Application::config`] 手动读取同名配置。
///
/// # 参数
///
/// - `components`：属性入口按源码顺序声明的组件列表。
/// - `tree`：需要检查的完整配置树；调用点使用最终树，从而同时覆盖本地与远端 overlay 引入的段。
pub(crate) fn warn_undeclared_sections(components: &[ComponentId], tree: &Value) {
    for (path, owner) in RESERVED_SECTIONS {
        if section_at(tree, path).is_none() || components.contains(owner) {
            continue;
        }
        tracing::warn!(
            "config section `{path}` is present but component `{owner}` is not declared; \
             the framework component will not apply it (business code may still read the raw config snapshot)"
        );
    }
}

/// 业务作用：在发布候选配置前校验所有已声明内置组件的配置段，阻止非法整帧替换有效快照。
///
/// 校验只做无副作用的反序列化：任一段非法时整帧候选都不发布，旧快照继续有效。
/// `application` 段不在此校验范围内——它是 bootstrap-only 的，运行期只做原始 section 比较。
///
/// 参数说明：
/// - `components`：属性入口声明的组件列表，决定哪些段属于“已声明”。
/// - `tree`：合并、插值完成但尚未对外发布的候选配置树。
/// - `phase`：本轮校验所属生命周期；启动首帧与运行期热刷新必须如实区分。
///
/// 返回：全部已声明段合法时成功；任一段非法时返回对应组件错误并保留旧快照。
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
            #[cfg(feature = "redis-job")]
            ComponentId::RedisJob => crate::redis_job::validate_redis_job_section(tree, phase)?,
            #[cfg(feature = "telemetry")]
            ComponentId::Telemetry => crate::telemetry::validate_telemetry_section(tree, phase)?,
            #[cfg(feature = "grpc")]
            ComponentId::Grpc => crate::grpc::validate_grpc_section(tree, phase)?,
            #[cfg(feature = "cache")]
            ComponentId::Cache => crate::cache::validate_cache_section(tree, phase)?,
            #[cfg(feature = "partition")]
            ComponentId::Partition => crate::partition::validate_partition_section(tree, phase)?,
            #[cfg(feature = "saga")]
            ComponentId::Saga => crate::saga::validate_saga_section(tree, phase)?,
            #[cfg(feature = "outbox")]
            ComponentId::Outbox => crate::outbox::validate_outbox_section(tree, phase)?,
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
    validate_managed_references(components, tree, phase)?;
    Ok(())
}

/// 业务作用：在任何组件产生网络副作用前复验内置计划引用的资源名称确实属于同一候选配置。
///
/// 单段反序列化只能证明 `datasource_ref`/`redis_ref` 语法合法，不能证明目标存在。本门禁在全部
/// 声明段完成无副作用校验后统一比对候选资源集合，避免 DB、Redis 或 Kafka 已开始握手后才发现引用
/// 指向不存在的实例。动态 UserHook 计划仍在其提交边界复验，因为此时尚未存在于配置树。
///
/// 参数说明：
/// - `components`：当前 Application 的冻结组件集合。
/// - `tree`：尚未触发建连的完整候选配置树。
/// - `phase`：启动首帧或运行期候选校验阶段。
///
/// 返回：所有静态资源引用均可由同一候选解析时成功；未知引用返回归属于消费组件的类型化错误。
fn validate_managed_references(
    components: &[ComponentId],
    tree: &Value,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    #[cfg(any(feature = "outbox", feature = "saga"))]
    let datasource_names = configured_datasource_names(tree);
    #[cfg(feature = "saga")]
    let user_hook_database = components.contains(&ComponentId::Saga)
        && tree
            .get("saga")
            .and_then(Value::as_object)
            .and_then(|settings| settings.get("database_bootstrap"))
            .and_then(Value::as_str)
            == Some("user_hook");

    #[cfg(feature = "outbox")]
    if components.contains(&ComponentId::Outbox) {
        let reference = string_setting(tree, "outbox", "datasource_ref").unwrap_or("default");
        #[cfg(feature = "saga")]
        let deferred_default = user_hook_database && reference == "default";
        #[cfg(not(feature = "saga"))]
        let deferred_default = false;
        if !deferred_default && !datasource_names.contains(reference) {
            return Err(crate::ApplicationError::new(
                ComponentId::Outbox,
                phase,
                format!(
                    "outbox.datasource_ref `{reference}` is not declared by database or datasources"
                ),
            ));
        }
    }

    #[cfg(feature = "saga")]
    if components.contains(&ComponentId::Saga) {
        let reference = string_setting(tree, "saga", "datasource_ref").unwrap_or("default");
        if !(datasource_names.contains(reference) || user_hook_database && reference == "default") {
            return Err(crate::ApplicationError::new(
                ComponentId::Saga,
                phase,
                format!(
                    "saga.datasource_ref `{reference}` is not declared by database or datasources"
                ),
            ));
        }
    }

    #[cfg(any(feature = "cache", feature = "scheduling"))]
    let redis_names = configured_redis_names(tree);

    #[cfg(feature = "cache")]
    if components.contains(&ComponentId::Cache) {
        if let Some(cache) = tree.get("cache").and_then(Value::as_object) {
            if cache.get("mode").and_then(Value::as_str) == Some("two_level") {
                if let Some(reference) = cache.get("redis_ref").and_then(Value::as_str) {
                    ensure_redis_reference(
                        &redis_names,
                        components.contains(&ComponentId::Redis),
                        reference,
                        ComponentId::Cache,
                        phase,
                    )?;
                }
            }
            if cache
                .get("invalidation")
                .and_then(Value::as_object)
                .and_then(|settings| settings.get("enabled"))
                .and_then(Value::as_bool)
                == Some(true)
            {
                if let Some(reference) = cache
                    .get("invalidation")
                    .and_then(Value::as_object)
                    .and_then(|settings| settings.get("redis_ref"))
                    .and_then(Value::as_str)
                {
                    ensure_redis_reference(
                        &redis_names,
                        components.contains(&ComponentId::Redis),
                        reference,
                        ComponentId::Cache,
                        phase,
                    )?;
                }
            }
        }
    }

    #[cfg(feature = "scheduling")]
    if components.contains(&ComponentId::Scheduling)
        && tree
            .get("scheduling")
            .and_then(Value::as_object)
            .and_then(|settings| settings.get("cluster"))
            .and_then(Value::as_str)
            == Some("leader")
    {
        let reference = string_setting(tree, "scheduling", "redis_ref").unwrap_or("default");
        ensure_redis_reference(
            &redis_names,
            components.contains(&ComponentId::Redis),
            reference,
            ComponentId::Scheduling,
            phase,
        )?;
    }

    let _ = (components, tree, phase);
    Ok(())
}

/// 业务作用：从候选 MySQL 配置提取业务可引用的规范化 datasource 名称。
///
/// 参数说明：`tree` 是已经通过 DB 段结构校验的候选配置。
///
/// 返回：单库形态只含 `default`，多库形态包含 map 的全部权威键，缺段时为空。
#[cfg(any(feature = "outbox", feature = "saga"))]
fn configured_datasource_names(tree: &Value) -> BTreeSet<&str> {
    if tree.get("database").is_some() {
        return BTreeSet::from(["default"]);
    }
    tree.get("datasources")
        .and_then(Value::as_object)
        .map(|sources| sources.keys().map(String::as_str).collect())
        .unwrap_or_default()
}

/// 业务作用：从候选 Redis 配置提取 Application 查询边界允许的 canonical 名称与默认别名。
///
/// 参数说明：`tree` 是已经通过 Redis 段结构与逐实例取值校验的候选配置。
///
/// 返回：扁平配置产生 `primary`/`default`，多实例配置保留命名键并为 `primary` 增加 `default`。
#[cfg(any(feature = "cache", feature = "scheduling"))]
fn configured_redis_names(tree: &Value) -> BTreeSet<String> {
    let Some(redis) = tree.get("redis").and_then(Value::as_object) else {
        return BTreeSet::new();
    };
    let mut names = BTreeSet::new();
    if let Some(properties) = redis.get("properties").and_then(Value::as_object) {
        for name in properties.keys() {
            let canonical = if name == "default" { "primary" } else { name };
            names.insert(canonical.to_owned());
            if canonical == "primary" {
                names.insert("default".to_owned());
            }
        }
    } else {
        names.insert("primary".to_owned());
        names.insert("default".to_owned());
    }
    names
}

/// 业务作用：把 Redis 静态引用与同一候选的资源集合比对，阻止消费组件回退默认实例。
///
/// 参数说明：
/// - `names`：候选 Redis 配置可发布的名称与兼容别名。
/// - `redis_declared`：当前 Application 是否真正声明 Redis 生命周期组件。
/// - `reference`：消费组件声明的显式资源引用。
/// - `component`：错误应归属的消费组件。
/// - `phase`：当前候选校验阶段。
///
/// 返回：引用存在时成功；未知名称返回不含 endpoint 或凭据的类型化错误。
#[cfg(any(feature = "cache", feature = "scheduling"))]
fn ensure_redis_reference(
    names: &BTreeSet<String>,
    redis_declared: bool,
    reference: &str,
    component: ComponentId,
    phase: ApplicationPhase,
) -> ApplicationResult<()> {
    if redis_declared && names.contains(reference) {
        return Ok(());
    }
    Err(crate::ApplicationError::new(
        component,
        phase,
        format!("redis_ref `{reference}` is not declared by redis or redis.properties"),
    ))
}

/// 业务作用：读取组件配置中的字符串引用，并让字段缺失继续采用各组件公开默认值。
///
/// 参数说明：`tree`、`section` 与 `field` 指向已完成结构校验的候选配置位置。
///
/// 返回：字段存在且为字符串时返回借用；缺失时返回 `None`。
#[cfg(any(feature = "outbox", feature = "saga", feature = "scheduling"))]
fn string_setting<'a>(tree: &'a Value, section: &str, field: &str) -> Option<&'a str> {
    tree.get(section)
        .and_then(Value::as_object)
        .and_then(|settings| settings.get(field))
        .and_then(Value::as_str)
}

/// 业务作用：判断某个组件负责的全部配置段在两棵树之间是否发生变化。
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
    RESERVED_SECTIONS
        .iter()
        .filter(|(_, owner)| *owner == component)
        .any(|(path, _)| section_at(current, path) != section_at(candidate, path))
}

/// 业务作用：按点分隔的固定配置路径读取子树，使独立组件可以只拥有共享根下的一个明确子段。
///
/// 参数说明：`tree` 为完整配置树，`path` 为编译期固定路径。
///
/// 返回：路径全部存在时返回对应子树；任一层缺失或不是对象时返回 `None`。
fn section_at<'a>(tree: &'a Value, path: &str) -> Option<&'a Value> {
    path.split('.')
        .try_fold(tree, |value, segment| value.get(segment))
}
