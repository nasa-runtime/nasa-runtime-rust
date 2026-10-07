# config-boot

`config-boot` 将 `naml` 的有序配置来源与 `nanacos` 的远端文本取得过程接起来。严格入口先求值连接所需的引导依赖，再把本地文件、自然排序的通配符组与 Nacos 文档按声明位置装配，最后求值嵌套表达式并绑定业务类型。兼容入口继续提供有序 overlay。

本组件不拥有应用配置发布权，也不建立独立的 secret 信任根。材料准备、观察循环、组件应用状态和停机由宿主负责；首拉与重载必须使用同一来源顺序，不能按远端通知到达顺序合并。

业务通常通过门面使用，不直接依赖本 crate。

```toml
[dependencies]
nasa = { version = "2.0.2", features = ["config-boot", "nacos-sdk"] }
```

## 入口选择

| 场景 | 入口与返回 |
| --- | --- |
| 显式严格装配 | `strict::prepare` 返回固定计划与连接设置，`strict::load` 首拉后返回 `LoadedConfig` |
| 已有完整远端包 | `strict::assemble` 校验包的身份、格式和次序，返回 `LoadedConfig` |
| 兼容 overlay 调用 | `resolve_ordered_overlays_for_bootstrap` / `assemble_overlays_from_bundle_for_bootstrap` 返回 `Vec<YmlOverlay>` |
| Application 受管应用 | 通过启动前配置工厂选择严格加载器，声明 `"nacos-config"`，由运行时管理首拉与刷新 |

严格入口的声明、格式、类型和预算规则见[严格分阶段装配](#严格分阶段装配)；不会自动改变已有 overlay 调用的合同。

## 兼容入口启动期加载

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

`config-boot` 读取启动期 `nacos:` 段。手动兼容入口可以把它放在 `zcf/bootstrap.yml`；Application 默认从 `zcf/application.yml` 取得，严格工厂也可显式指定主路径。下表中的默认值用于兼容 `NacosBootstrap`；严格 map import 必须显式提供布尔 `optional`，不能依赖表中的缺省值。

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

## Application 配置架构

门面开启 `application,nacos-config,nacos-sdk` 并声明 `"nacos-config"` 时，Application 负责首拉、
远端观察和停机；业务不再重复安装 watcher。额外开启 `yml-watch` 并设置 `config_watch.enabled`
可观察本地来源。两类变化都按同一 import 顺序重建候选，本地更新保留当前有效的远端 overlay。

```text
本地 bootstrap → 有序 import 与远端文本 → naml 合并与插值 → 完整候选
                                                             ↓
                                       材料及资源准备 → 发布 ConfigView
```

`config-boot` 的兼容入口产出 overlay，严格入口产出带来源信息的 `LoadedConfig`；secret、TLS、日志资源的准备与实际配置应用状态由 Application
管理。候选失败保留旧视图，不能把收到远端通知当作业务资源已经切换的证明。配置中心引导凭据
必须有独立信任根，不能依赖尚待该配置中心提供的材料。

## 主要边界

- bootstrap 只负责定位和装配配置来源；最终业务配置仍由 `naml` 完整反序列化并校验。
- overlay 顺序是合同，热刷新必须复用启动时的 import 顺序，不能按到达顺序重排。
- `enabled: true` 时至少要有一个远端 import；未知格式、旧字段和缺失的必需文档都应 fail-fast。
- 用户名、密码和远端配置正文不得进入日志或公开错误；敏感值应由环境变量或部署信任根注入。

## 严格分阶段装配

`strict::prepare(&ConfigLoader)` 复用 naml 的来源计划，只求值配置中心引导依赖；返回固定 `PreparedLoad` 与 `NacosBootstrap`。`strict::load(plan, client, boot)` 首拉远端文本，`strict::assemble(plan, bundle, boot)` 接收已有完整包。两者最终使用同一 naml 装配器，不另行实现 glob、格式解析或类型推断。

```rust
use nasa::yml::{strict::ConfigLoader, nacos};

/// 业务作用：取得完整本地与远端配置后再绑定业务字段。
/// 参数说明：无。
/// 返回：来源齐全且字段符合目标类型时返回配置，否则保留加载或绑定错误。
async fn load_settings<T: serde::de::DeserializeOwned>() -> anyhow::Result<T> {
    let loader = ConfigLoader::standard()?;
    let (plan, bootstrap) = nacos::strict::prepare(&loader)?;
    let loaded = if bootstrap.enabled {
        let client = nacos::connect_config_client(&bootstrap).await?;
        nacos::strict::load(plan, &client, &bootstrap).await?
    } else {
        plan.finish(&std::collections::BTreeMap::new())?
    };
    Ok(loaded.bind()?)
}
```

`nacos.imports` 位于 `yml.imports` 之前；精确文件、各自自然排序的 glob 组和 Nacos 文档保持声明位置。规范化远端身份不得重复，包中不能存在多余、乱序、格式不一致或缺失的必需文档。可选仅允许明确不存在；取得的坏内容仍失败。严格 map 声明显式给出 optional，未知字段拒绝，禁用配置中心不访问其未启用连接字段。

通配符只作用于本地文件名，不枚举 Nacos data ID。连接启用但没有远端来源、连接禁用却仍声明远端来源均拒绝；可选文档不能掩盖连接或鉴权失败。诊断不应记录配置正文、环境值或凭据。

同一预读文档进入最终装配，后续 overlay 覆盖的业务占位符不会阻断首拉；导入文档不能修改来源控制与 provider 信任根。完整来源身份在返回候选前复验。读取预算、模式边界、纯内存入口和诊断合同见 [naml](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/naml/README.md)。旧 overlay 入口保留其读取与兼容语义，不会因本接口增加而自动执行新权限。
