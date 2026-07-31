# config-boot

`config-boot` 是 `naml` YAML 分层加载器和 `nanacos` 配置中心之间的引导胶水。`naml` 只负责本地文件、profile、overlay、环境变量和占位符解析；`nanacos` 只负责按 `data_id/group` 拉取裸文本。`config-boot` 把 `yml.imports` / `nacos.imports` 解析成有序 overlay，供业务最终反序列化成强类型配置。

业务通常通过门面使用，不直接依赖本 crate。

```toml
[dependencies]
nasa = { version = "1", features = ["config-boot", "nacos-sdk"] }
```

## 启动期加载

适合应用启动时读取 bootstrap，本地 import 和 Nacos import 按声明顺序合并。

```rust
use nasa::yml::nacos::{
    connect_config_client, load_bootstrap_checked, resolve_imports,
    resolve_ordered_overlays_for_bootstrap,
};
use nasa::yml::YmlLoader;

#[derive(serde::Deserialize)]
struct AppConfig {
    app_name: String,
}

#[derive(serde::Deserialize)]
struct BootstrapConfig {
    nacos: nasa::yml::nacos::NacosBootstrap,
}

async fn load() -> anyhow::Result<AppConfig> {
    let boot_loader = YmlLoader::standard().base_file("config/bootstrap.yml");
    let bootstrap: BootstrapConfig = load_bootstrap_checked(&boot_loader)?;
    let boot_tree = boot_loader.load_tree()?;
    let imports = resolve_imports(&boot_tree, boot_loader.base_file_dir(), &bootstrap.nacos)?;
    let client = connect_config_client(&bootstrap.nacos).await?;
    let overlays = resolve_ordered_overlays_for_bootstrap(&client, &imports, &bootstrap.nacos).await?;

    YmlLoader::standard()
        .base_file("config/application.yml")
        .load_with_overlays(&overlays)
}
```

## 热刷新重组

适合 `nanacos::watch_many_channel` 推送 `ConfigBundle` 后，按原 import 顺序重建 overlay，再交给 `naml` 重新加载。

```rust
use nasa::yml::nacos::assemble_overlays_from_bundle_for_bootstrap;

async fn rebuild_overlays(
    imports: &[nasa::yml::YmlImport],
    bundle: &nasa::config::nacos::ConfigBundle,
    nacos: &nasa::yml::nacos::NacosBootstrap,
) -> anyhow::Result<Vec<nasa::yml::YmlOverlay>> {
    assemble_overlays_from_bundle_for_bootstrap(imports, bundle, nacos).await
}
```

## import 格式规则

远端 Nacos 文档格式按以下优先级决定：

1. `import.file_extension`
2. `nacos.file_extension`
3. 默认 `yaml`

本地 `file:` import 的格式从文件后缀推断。显式配置未知格式会 fail-fast，不会猜测。

## 旧字段守卫

`reject_legacy_config_fields` 用于拒绝旧式单 `data_id` 配置，避免绕过新的 import 模型。

```rust
let boot_value: serde_json::Value = serde_json::from_str(raw)?;
nasa::yml::nacos::reject_legacy_config_fields(&boot_value)?;
```

## YML 配置与使用

`config-boot` 读取的是启动期 `nacos:` 段，通常放在 `zcf/bootstrap.yml`。它只负责把本地 bootstrap 中的 Nacos 连接参数和 import 清单转成 `naml::YmlOverlay`，最终业务配置仍由 `naml` 反序列化。

完整示例：

```yaml
nacos:
  enabled: true
  server_addr: 127.0.0.1:8848
  namespace: ""
  group: DEFAULT_GROUP
  app_name: order-service
  discovery_ip: 127.0.0.1
  username: ${NACOS_USERNAME:}
  password: ${NACOS_PASSWORD:}
  file_extension: yaml
  imports:
    - data_id: common.yml
      group: DEFAULT_GROUP
      optional: false
      file_extension: yaml
    - data_id: order-service.yml
      optional: false
```

字段说明：

| 键 | 默认值 | 说明 |
| --- | --- | --- |
| `nacos.enabled` | `false` | `true` 时允许连接配置中心；`false` 时应走纯本地配置。 |
| `nacos.server_addr` | `""` | Nacos SDK 地址，通常是 `host:8848`；开启后不能为空。 |
| `nacos.namespace` | `""` | namespace ID；public 空间留空。 |
| `nacos.group` | `DEFAULT_GROUP` | import 未显式 group 时的默认分组。 |
| `nacos.app_name` | `""` | Nacos 端展示和审计用应用名。 |
| `nacos.discovery_ip` | `null` | 仅服务注册场景使用；配置中心拉取可不填。 |
| `nacos.username` | `""` | 鉴权用户名。 |
| `nacos.password` | `""` | 鉴权密码；建议用 `APP__NACOS__PASSWORD` 注入。 |
| `nacos.file_extension` | `yaml` | imports 的默认格式，可选 `yaml`/`yml`/`json`/`toml`。 |
| `nacos.imports[].data_id` | 必填 | 远端配置 data id。 |
| `nacos.imports[].group` | 回退 `nacos.group` | 单条 import 分组。 |
| `nacos.imports[].optional` | `true` | `false` 表示缺失即启动失败。 |
| `nacos.imports[].file_extension` | 回退全局格式 | 单条 import 的内容格式。 |

启动期推荐流程：

```rust
let boot_loader = nasa::yml::YmlLoader::standard().base_file("zcf/bootstrap.yml");
let bootstrap: BootstrapConfig =
    nasa::yml::nacos::load_bootstrap_checked(&boot_loader)?;

if bootstrap.nacos.enabled {
    let tree = boot_loader.load_tree()?;
    let imports = nasa::yml::nacos::resolve_imports(
        &tree,
        boot_loader.base_file_dir(),
        &bootstrap.nacos,
    )?;
    let client = nasa::yml::nacos::connect_config_client(&bootstrap.nacos).await?;
    let overlays = nasa::yml::nacos::resolve_ordered_overlays_for_bootstrap(
        &client,
        &imports,
        &bootstrap.nacos,
    )
    .await?;
    let cfg: AppConfig =
        nasa::yml::YmlLoader::standard().load_with_overlays(&overlays)?;
}
```

热刷新时继续复用同一 import 顺序：`nacos_refs_for_bootstrap` 生成监听列表，`watch_many_channel` 收到 `ConfigBundle` 后调用 `assemble_overlays_from_bundle_for_bootstrap`，再重新 `load_with_overlays`。

## 主要边界

- bootstrap 只负责定位和装配配置来源；最终业务配置仍由 `naml` 完整反序列化并校验。
- overlay 顺序是合同，热刷新必须复用启动时的 import 顺序，不能按到达顺序重排。
- `enabled: true` 时至少要有一个远端 import；未知格式、旧字段和缺失的必需文档都应 fail-fast。
- 用户名、密码和远端配置正文不得进入日志或公开错误；敏感值应由环境变量或部署信任根注入。
