//! 同一应用 YAML 的独立平台 controller；不启动业务 listener 或 SQL 资源。

use nafana::observability::{platform::PlatformController, ObservabilityConfig, ProvisioningMode};
use naml::YmlLoader;
use serde::Deserialize;
use serde_json::Value;
use std::{path::Path, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

/// 业务作用：启动只有控制面权限的独立平台进程。
/// 参数说明：命令行提供应用 YAML 路径，可选 --once 执行一次收敛。
/// 返回：配置或 required 观测阻断时以非零码退出，不触碰业务进程。
#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("nafana controller: {error}");
        std::process::exit(1);
    }
}

/// 业务作用：解析同一 YAML、平台 binding 和唯一 Grafana secret 后接管调和生命周期。
/// 参数说明：无；输入只来自进程命令行与平台标准环境。
/// 返回：停机或一次成功调和时成功；最终关闭时不创建平台资源，已启用的配置中心仍先完成读取。
async fn run() -> Result<(), String> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.is_empty() || args.len() > 2 || args.get(1).is_some_and(|v| v != "--once") {
        return Err("usage: nafana-controller <application.yaml> [--once]".into());
    }
    let root = load_effective_config(Path::new(&args[0])).await?;
    let disabled = root
        .pointer("/grafana/observability/enabled")
        .is_none_or(|value| value == false)
        || root
            .pointer("/grafana/observability/provisioning/mode")
            .is_none_or(|value| value == "disabled");
    if disabled {
        // 必须先完成配置来源合并，远端和环境覆盖也可能启用 platform；最终关闭时不解析材料或申请 owner 权威。
        ObservabilityConfig::from_root(&root)?;
        return Ok(());
    }
    let mut projection = root.clone();
    if let Some(value) = projection.pointer_mut("/grafana/observability") {
        // controller 不借用任一业务 Pod 的实例主键；资源 UID 仅由服务、环境和集群派生。
        if let Some(identity) = value.get_mut("identity").and_then(Value::as_object_mut) {
            identity.insert(
                "instance_id".into(),
                Value::String("platform-controller".into()),
            );
        }
        require_resolved(value)?;
    }
    let config = ObservabilityConfig::from_root(&projection)?;
    if config.scrape_enabled()
        && config.prometheus.scrape.listener == nafana::observability::ListenerMode::Web
    {
        if let Some(server) = projection.get_mut("server") {
            for key in ["port", "context_path"] {
                if let Some(value) = server.get_mut(key) {
                    require_resolved(value)?;
                }
            }
        }
    }
    let config = ObservabilityConfig::platform_projection(&projection)?;
    if !config.enabled || config.provisioning.mode != ProvisioningMode::Platform {
        return Ok(());
    }
    if let Some(name) = root.pointer("/application/name") {
        require_resolved(name)?;
    }
    // 服务主键先遵循有效配置；平台 binding 只能填补缺省，不能把控制面资源归给另一个服务。
    let service = if !config.identity.service_name.is_empty() {
        config.identity.service_name.clone()
    } else if let Some(name) = root.pointer("/application/name") {
        name.as_str()
            .ok_or("application.name must be a string")?
            .to_owned()
    } else {
        std::env::var("NAFANA_SERVICE_NAME")
            .map_err(|_| "application service name binding is required")?
    };
    let identity =
        config.freeze_identity(&service, "platform-controller", &serde_json::json!({}))?;
    let mut bindings = config.provisioning.bindings.clone();
    macro_rules! binding {
        ($field:ident,$env:literal) => {
            if bindings.$field.is_none() {
                bindings.$field = std::env::var($env).ok();
            }
        };
    }
    binding!(grafana_endpoint, "NAFANA_GRAFANA_ENDPOINT");
    binding!(grafana_token_secret, "NAFANA_GRAFANA_TOKEN_SECRET");
    binding!(prometheus_url, "NAFANA_PROMETHEUS_URL");
    binding!(docker_dns, "NAFANA_DOCKER_DNS");
    binding!(prometheus_reload_url, "NAFANA_PROMETHEUS_RELOAD_URL");
    binding!(scrape_token_file, "NAFANA_SCRAPE_TOKEN_FILE");
    binding!(scrape_token_secret, "NAFANA_SCRAPE_TOKEN_SECRET");
    binding!(namespace, "POD_NAMESPACE");
    binding!(kubernetes_api, "NAFANA_KUBERNETES_API");
    macro_rules! path_binding {
        ($field:ident,$env:literal) => {
            if bindings.$field.is_none() {
                bindings.$field = std::env::var_os($env).map(std::path::PathBuf::from);
            }
        };
    }
    path_binding!(owner_lock_file, "NAFANA_OWNER_LOCK_FILE");
    path_binding!(prometheus_config, "NAFANA_PROMETHEUS_CONFIG");
    path_binding!(kubernetes_token_file, "NAFANA_KUBERNETES_TOKEN_FILE");
    path_binding!(kubernetes_ca_file, "NAFANA_KUBERNETES_CA_FILE");
    if bindings.kubernetes_api.is_none() {
        if let Ok(host) = std::env::var("KUBERNETES_SERVICE_HOST") {
            let port =
                std::env::var("KUBERNETES_SERVICE_PORT_HTTPS").unwrap_or_else(|_| "443".into());
            bindings.kubernetes_api = Some(format!("https://{host}:{port}"));
        }
    }
    if bindings.kubernetes_api.is_some() {
        bindings
            .kubernetes_token_file
            .get_or_insert_with(|| "/var/run/secrets/kubernetes.io/serviceaccount/token".into());
        bindings
            .kubernetes_ca_file
            .get_or_insert_with(|| "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt".into());
        if bindings.namespace.is_none() {
            bindings.namespace =
                std::fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
                    .ok()
                    .map(|v| v.trim().to_owned());
        }
    }
    let locator = config
        .provisioning
        .grafana
        .api_token
        .as_deref()
        .or(bindings.grafana_token_secret.as_deref())
        .ok_or("Grafana credential locator binding is required")?;
    let id = nafana::observability::secret_id(locator)?;
    let token = resolve_secret(&root, id)?;
    let controller = PlatformController::new(config, identity, bindings, token)?;
    if args.get(1).is_some() {
        return controller.reconcile().await;
    }
    let stop = CancellationToken::new();
    tokio::select! {result=controller.run(stop.clone())=>result,_=tokio::signal::ctrl_c()=>{stop.cancel();Ok(())}}
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SecretConfig {
    encoding: String,
    max_bytes: usize,
    fragments: Vec<Fragment>,
}
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Fragment {
    Env(String),
    File(std::path::PathBuf),
    ConfigPath(String),
}

/// 业务作用：只解析 controller 实際使用的一份 secret，不读取 SQL 或通知材料。
/// 参数说明：`root` 为按标准优先级合并后的配置，`id` 为已验证的 locator 标识。
/// 返回：受硬上限约束的材料；错误不含凭据或 fragment 值。
fn resolve_secret(root: &Value, id: &str) -> Result<nasecret::SecretBytes, String> {
    let declaration = root
        .get("secrets")
        .and_then(|v| v.get(id))
        .ok_or("Grafana secret declaration is missing")?;
    require_resolved(declaration)?;
    let config: SecretConfig = serde_json::from_value(declaration.clone())
        .map_err(|_| "Grafana secret declaration is invalid")?;
    let encoding = match config.encoding.as_str() {
        "raw" => nasecret::SecretEncoding::Raw,
        "base64_after_concat" => nasecret::SecretEncoding::Base64AfterConcat,
        "hex_after_concat" => nasecret::SecretEncoding::HexAfterConcat,
        _ => return Err("Grafana secret encoding is invalid".into()),
    };
    let fragments = config
        .fragments
        .into_iter()
        .map(|f| match f {
            Fragment::Env(v) => nasecret::SecretFragmentRef::Env(Arc::from(v)),
            Fragment::File(v) => nasecret::SecretFragmentRef::File(v),
            Fragment::ConfigPath(v) => nasecret::SecretFragmentRef::ConfigPath(Arc::from(v)),
        })
        .collect();
    let spec = nasecret::SecretSpec {
        id: Arc::from(id),
        encoding,
        max_bytes: config.max_bytes,
        fragments,
    };
    spec.resolve(|path| {
        if path.starts_with("secrets.") {
            return None;
        }
        let mut current = root;
        for segment in path.split('.') {
            current = current.get(segment)?;
        }
        require_resolved(current).ok()?;
        current.as_str().map(Arc::from)
    })
    .map_err(|_| format!("Grafana secret {id} cannot be resolved"))
}

/// 业务作用：在消费控制面字段前拒绝尚未解析的引用，业务独有的未挂载材料不进入此门禁。
/// 参数说明：`value` 是已经经标准加载器完成可用引用解析的控制面字段。
/// 返回：没有残留占位符时成功；错误只含固定分类，不回显配置、变量或凭据。
fn require_resolved(value: &Value) -> Result<(), String> {
    match value {
        Value::Object(values) => {
            for value in values.values() {
                require_resolved(value)?;
            }
        }
        Value::Array(values) => {
            for value in values {
                require_resolved(value)?;
            }
        }
        Value::String(text) if text.contains("${") => {
            return Err("controller configuration contains an unresolved placeholder".into());
        }
        _ => {}
    }
    Ok(())
}

/// 业务作用：按应用配置优先级取得 controller 启动快照，再允许后续判断平台启用状态。
/// 参数说明：`path` 指定主配置，活动 profile 从同目录的主文件名加 -{APP_PROFILE} 读取。
/// 返回：主文件、profile、启用的 Nacos/file imports、APP__ 覆盖依次合并；失败不返回部分配置。
/// 未命中的业务独有引用暂留，控制面使用的字段随后严格检查，避免要求挂载数据库或通知凭据。
async fn load_effective_config(path: &Path) -> Result<Value, String> {
    let stem = path
        .file_stem()
        .and_then(|name| name.to_str())
        .ok_or("application configuration path is invalid")?;
    let profile = path.with_file_name(format!("{stem}-{{profile}}"));
    let loader = YmlLoader::standard()
        .base_file(path)
        .profile_file_pattern(
            profile
                .to_str()
                .ok_or("application profile path is invalid")?,
        )
        .max_file_bytes(4 * 1024 * 1024)
        .preserve_unresolved_placeholders(true);
    let local = loader
        .load_tree()
        .map_err(|_| "application configuration cannot be loaded")?;
    let bootstrap = local
        .get("nacos")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let boot: config_boot::NacosBootstrap = serde_json::from_value(bootstrap.clone())
        .map_err(|_| "nacos bootstrap configuration is invalid")?;
    if !boot.enabled {
        // 与应用的配置中心开关一致；关闭时不读取 imports 或连接配置中心。
        return Ok(local);
    }
    require_resolved(&bootstrap)?;
    if let Some(imports) = local.get("yml") {
        require_resolved(imports)?;
    }
    if !cfg!(feature = "controller-nacos") {
        // 未编译真实传输时拒绝继续，不能把未取得的远端覆盖当作不存在。
        return Err("nacos configuration requires the controller-nacos feature".into());
    }
    let imports = config_boot::resolve_imports(&local, loader.base_file_dir(), &boot)
        .map_err(|_| "nacos imports are invalid")?;
    config_boot::validate_imports_enabled(&boot, &imports)
        .map_err(|_| "nacos imports are invalid")?;
    let timeout_ms = match local.pointer("/application/startup_timeout_ms") {
        Some(value) => value
            .as_u64()
            .filter(|value| (1..=31_536_000_000).contains(value))
            .ok_or("application.startup_timeout_ms is invalid")?,
        None => 30_000,
    };
    let merged = tokio::time::timeout(Duration::from_millis(timeout_ms), async {
        let client = config_boot::connect_config_client(&boot)
            .await
            .map_err(|_| "nacos configuration connection failed")?;
        let overlays =
            config_boot::resolve_ordered_overlays_for_bootstrap(&client, &imports, &boot)
                .await
                .map_err(|_| "nacos configuration imports could not be loaded")?;
        if overlays
            .iter()
            .any(|overlay| overlay.content.len() > 4 * 1024 * 1024)
        {
            return Err("configuration overlay exceeds size limit");
        }
        loader
            .load_tree_with_overlays(&overlays)
            .map_err(|_| "application configuration overlays could not be merged")
    })
    .await
    .map_err(|_| "controller configuration bootstrap timed out")??;
    // application 决定应用的启动身份与预算；远端不能改写已钉住的本地启动合同。
    if merged.get("application") != local.get("application") {
        return Err("bootstrap-only configuration conflict: imports changed application".into());
    }
    Ok(merged)
}
