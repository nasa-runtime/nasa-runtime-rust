# naml

`naml` 是通用分层配置加载器。它负责本地配置、profile 配置、内存 overlay、环境变量和 `${...}` 占位符解析，最终反序列化成业务强类型配置。它不连接 Nacos，也不持有全局状态；Nacos 拉取由 `config-boot` / `nanacos` 完成。

业务项目通过门面开启 `yml`：

```toml
[dependencies]
nasa = { version = "1", features = ["yml"] }
```

## 本地文件加载

```rust
use nasa::yml::YmlLoader;

#[derive(serde::Deserialize)]
struct AppConfig {
    server: ServerConfig,
}

#[derive(serde::Deserialize)]
struct ServerConfig {
    port: u16,
}

fn load() -> anyhow::Result<AppConfig> {
    YmlLoader::standard()
        .base_file("config/application.yml")
        .load()
}
```

## profile 配置

适合 `application.yml` + `application-dev.yml` 这种场景，后者覆盖前者。

```rust
std::env::set_var("APP_PROFILE", "dev");

let cfg: AppConfig = nasa::yml::YmlLoader::standard()
    .base_file("config/application.yml")
    .load()?;
```

## overlay 配置

适合把 Nacos 或内存配置插入到本地配置之后继续合并。

```rust
use nasa::yml::{ConfigFormat, YmlLoader, YmlOverlay};

let remote = YmlOverlay::required(
    "nacos:app.yml",
    "server:\n  port: 18080\n",
    ConfigFormat::Yaml,
);

let cfg: AppConfig = YmlLoader::standard()
    .base_file("config/application.yml")
    .load_with_overlays(&[remote])?;
```

## import 解析

`naml` 只把 import 解析成中性描述，不拉远端。

```rust
let loader = nasa::yml::YmlLoader::standard().base_file("config/bootstrap.yml");
let tree = loader.load_tree()?;
let imports: Vec<nasa::yml::YmlImport> =
    nasa::yml::parse_imports_from_tree(&tree, loader.base_file_dir());
```

`YmlImport::File` 由本地文件加载，`YmlImport::Nacos` 交给 `config-boot` 转成 `nanacos::ConfigRef`。

## 边界

- 支持 yaml/json/toml 三种格式。
- 合并是叶子级深合并，后加载的 overlay 覆盖前面的同名叶子。
- `${...}` 占位符解析在最终树上执行。
- 不做热替换策略；热刷新由应用收到新 overlay 后重新调用 loader。

## YML 配置与使用

`naml` 没有固定业务根节点，它会把应用自己的 yml 反序列化成任意 `serde::Deserialize` 结构。默认本地文件是 `zcf/application.yml`；只有显式设置 `APP_PROFILE` 时才加载 `application-{profile}.yml`；环境变量 `APP__A__B` 会覆盖 `a.b`。

完整示例：

```yaml
server:
  host: 0.0.0.0
  port: 8080

mysql:
  url: ${APP_MYSQL_URL}
  max_connections: 16

redis:
  url: ${APP_REDIS_URL}
  namespace: order-service
  profile: RustV2

yml:
  imports:
    - file: common.yml
      optional: false
    - nacos:
        data_id: order.yml
        group: DEFAULT_GROUP
        optional: true
        file_extension: yaml
```

字段说明：

| 键 | 说明 |
| --- | --- |
| `APP_PROFILE` | 环境变量；非空时额外加载 `application-{profile}.yml`。 |
| `APP__X__Y` | 环境变量覆盖 `x.y`，优先级最高。 |
| `yml.imports[].file` | 本地 overlay 文件路径；相对路径按主配置文件目录解析。 |
| `yml.imports[].nacos.data_id` | 远端配置 data id；只被解析为中性 import，不在本 crate 内拉取。 |
| `optional` | `false` 表示源缺失或内容为空即启动失败；`true` 表示缺失可跳过。 |
| `file_extension` | 覆盖内容格式，可选 `yaml`/`yml`/`json`/`toml`。 |

启动代码：

```rust
#[derive(serde::Deserialize)]
struct AppConfig {
    server: ServerConfig,
}

#[derive(serde::Deserialize)]
struct ServerConfig {
    host: String,
    port: u16,
}

let cfg: AppConfig = nasa::yml::YmlLoader::standard().load()?;
```

`naml` 只负责读本地、合并 overlay、解析占位符和反序列化。涉及 Nacos 的拉取、watch 和 overlay 重组时，应用应组合 `config-boot` 与 `nanacos`。
