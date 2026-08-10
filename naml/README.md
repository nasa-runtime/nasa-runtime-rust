# naml

`naml` 是通用分层配置加载器与本地配置变化观察组件。它把主配置、profile、内存 overlay 和环境变量按固定优先级合并，解析 `${...}` 占位符后反序列化成业务强类型配置；tracked 入口同时返回本轮实际来源，避免加载、指纹和 watcher 各自猜测 profile 路径。

本地文件可设置单文件读取上限，读取与上限判定使用同一文件句柄。可选 `watch` feature 监听精确目标的父目录，既能观察临时文件加原子 rename，又不会让同目录日志或无关文件触发配置事件。

`naml` 不连接 Nacos，也不持有或发布应用的活动配置。watcher 只报告变化；候选校验、运行态资源准备、原子发布、回滚和审计由应用负责。Nacos 拉取与 overlay 组装由 `config-boot` / `nanacos` 完成。

直接使用加载能力：

```toml
[dependencies]
naml = "1.0.2"
```

需要文件变化观察时显式开启 `watch` feature：

```toml
[dependencies]
naml = { version = "1.0.2", features = ["watch"] }
```

## 运行架构与顺序不变量

加载流程分为四步：先解析主文件与活动 profile 的确定来源，再按“主文件 < profile < 内存 overlay < 环境变量”的固定优先级做叶子级深合并，随后解析 `${...}` 占位符，最后反序列化为业务类型。`load_tracked` 在返回业务值的同时返回这一轮实际来源，使指纹、归档和 watcher 使用同一来源事实。

watcher 与加载流程相互独立。它以非递归方式观察精确目标的父目录，把底层事件分类为 `Base`、`Profile` 或 `Dependency`；应用收到事件后重新加载和校验候选配置，只有候选可用时才准备资源并发布。成功加载后可调用 `reconcile` 更新来源与附加依赖。

`reconcile` 先挂载全部新增目录，再一次发布新的精确目标，最后撤销失效目录。新增目录监听失败时旧目标和旧观察保持有效；撤销旧目录失败只留下被精确过滤的额外观察，不会把旧文件重新认作配置来源。符号链接路径同时保留声明节点与当前真实来源，因此 Kubernetes ConfigMap 换代、文件链接替换和真实文件修改都能进入同一重新加载流程。

`naml` 不提供候选配置去抖、业务校验、运行态资源准备、原子配置发布或回滚。应用必须在自身配置发布边界内完成这些动作，不能把收到文件事件等同于新配置已经生效。

## 本地文件加载

```rust
use naml::YmlLoader;

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
        .max_file_bytes(16 * 1024 * 1024)
        .load()
}
```

## 来源追踪

需要计算配置指纹、归档或建立热更新 watcher 时，使用 tracked 入口，不要在应用里重复推导 profile：

```rust
use naml::YmlLoader;

let loader = YmlLoader::standard()
    .base_file("config/application.yml")
    .profile_file_pattern("config/application-{profile}")
    .max_file_bytes(16 * 1024 * 1024);

let loaded = loader.load_tracked::<AppConfig>()?;
for path in loaded.sources.loaded_files() {
    println!("loaded {}", path.display());
}
let config = loaded.value;
```

`loaded_files()` 只包含本轮实际读取的主文件和活动 profile；`watch_files()` 还包含当前 profile 可能在未来创建的格式候选。

## 文件变化观察

启用 `watch` feature 后，可以把 naml 来源与应用自己的证书、策略或 schema 文件放进同一个精确 watcher：

```rust
use naml::watch::YmlWatcher;
use naml::YmlLoader;

let loader = YmlLoader::standard().base_file("config/application.yml");
let sources = loader.local_sources()?;
let dependencies = vec!["config/server.pem".into()];
let (event_sender, event_receiver) = std::sync::mpsc::channel();
let mut watcher = YmlWatcher::new(&sources, &dependencies, move |event| {
    let _ = event_sender.send(event);
})?;

// 应用自己的去抖工作线程消费事件，再执行候选解析、校验和原子发布。
let event = event_receiver.recv()?;
println!("configuration source changed: {:?}", event.kind);

// 成功解析新配置后再对账依赖；新增目录监听失败时旧目标仍保持有效。
let next_sources = loader.local_sources()?;
let next_dependencies = vec!["config/rotated/server.pem".into()];
watcher.reconcile(&next_sources, &next_dependencies)?;
```

底层文件系统可能为一次写入报告多个事件，因此 handler 应执行去抖和内容去重。`YmlWatcher` 不自动调用 loader，也不替应用发布配置。

## profile 配置

适合 `application.yml` + `application-dev.yml` 这种场景，后者覆盖前者。`base_file` 只修改主文件；
主文件不在默认 `zcf/` 目录时，还必须同步设置 profile pattern。

profile pattern 指向的精确文件如果已经存在，文件必须带 `yaml`、`yml`、`json` 或 `toml` 扩展名；
缺少扩展名或格式不受支持时加载失败，不会静默回落到其它候选。

```rust
# 启动进程前设置：APP_PROFILE=dev

let cfg: AppConfig = naml::YmlLoader::standard()
    .base_file("config/application.yml")
    .profile_file_pattern("config/application-{profile}")
    .load()?;
```

## overlay 配置

适合把 Nacos 或内存配置插入到本地配置之后继续合并。

```rust
use naml::{ConfigFormat, YmlLoader, YmlOverlay};

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
let loader = naml::YmlLoader::standard().base_file("config/bootstrap.yml");
let tree = loader.load_tree()?;
let imports: Vec<naml::YmlImport> =
    naml::parse_imports_from_tree(&tree, loader.base_file_dir());
```

`YmlImport::File` 由本地文件加载，`YmlImport::Nacos` 交给 `config-boot` 转成 `nanacos::ConfigRef`。

## 边界

- 支持 yaml/json/toml 三种格式。
- 合并是叶子级深合并，后加载的 overlay 覆盖前面的同名叶子。
- `${...}` 占位符解析在最终树上执行。
- `max_file_bytes` 分别约束主文件和活动 profile，不约束调用方提供的内存 overlay。
- 本地文本与底层 `config` 文件源保持一致：移除 UTF-8 BOM，非法 UTF-8 字节替换为 U+FFFD 后再解析。
- 默认不引入 `notify`；只有显式开启 `watch` feature 才编译文件观察能力。
- 不做热替换策略；热刷新由应用收到文件事件或新 overlay 后重新调用 loader。

## 观测与失败语义

- watcher handler 收到来源类别和命中的事件路径；一次写入可能产生多个底层事件，组件不承诺恰好一次，应用应按内容指纹去抖。
- watcher 后端异常、增量监听回退异常和旧目录撤销异常通过 `tracing` 记录；组件不内置指标、日志订阅器或告警系统。
- 主文件缺失、读取失败、超过 `max_file_bytes`、格式不受支持、必需 overlay 无效或强类型反序列化失败时，加载不返回部分配置。
- 精确 profile 路径已存在但缺少受支持扩展名时失败闭合；不存在的 profile 仍是可选来源，其格式候选会进入 watcher 目标。
- watcher 初始化无法观察任一必需目录时整体失败；`reconcile` 无法观察新增目录时返回错误并保留旧目标。

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
  profile: production

yml:
  imports:
    - file: common.yml
      optional: false
    - nacos: order.yml
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
| `yml.imports[].nacos` | 远端配置 data id 字符串；只被解析为中性 import，不在本 crate 内拉取。 |
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

let cfg: AppConfig = naml::YmlLoader::standard().load()?;
```

`naml` 只负责读本地、合并 overlay、解析占位符和反序列化。涉及 Nacos 的拉取、watch 和 overlay 重组时，应用应组合 `config-boot` 与 `nanacos`。
