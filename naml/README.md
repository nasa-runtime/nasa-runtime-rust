# naml

`naml` 将文件、内存文档和环境变量装配成可绑定的配置候选，支持嵌套 `${...}` 默认值、有序本地导入与文件名通配符。严格入口同时返回字段来源、依赖关系、值指纹和目录观察计划，使加载、解释与观察使用同一轮来源事实。

它不连接配置中心、不读取 secret provider、不发布应用运行态。Nacos 文本由 `config-boot` 提供；监听事件只表示需要重读。业务校验、资源准备、发布和各组件应用结果由宿主负责。

## 装配架构与职责

```text
固定环境、主路径与 LoadPolicy
  → 读取主文件和 profile → 校验并冻结有序来源计划
  → 本地精确文件 / 自然排序的 glob 组 / 调用方提供的远端文档
  → 按声明顺序合并 → APP__ 覆盖 → 求值嵌套表达式 → 按目标类型绑定
  → LoadedConfig：配置值、来源解释、指纹和观察计划
```

来源计划决定允许读取什么，完整合并后的树决定业务表达式引用什么。需要配置中心时，
`prepare` 先提供来源与连接所需的引导值，`finish` 在远端文本齐全后求值业务树；不会因被后续来源
覆盖的业务占位符尚未命中而提前中止首拉。来源身份、读取预算或表达式校验失败时不返回部分候选。

`LoadedConfig` 是一次装配结果。宿主先完成业务校验、材料准备和观察集准备，再决定是否发布；
文件事件和指纹都不代表业务资源已经应用配置。内存文档入口完全由调用方供给材料，不根据其中的
`yml.imports` 自动扩大读取权限。

## 安装与入口选择

```toml
[dependencies]
naml = { version = "2.0.1", features = ["watch"] }
```

默认 feature 不包含文件监听。同步解析与装配不需要 Tokio 或 Nacos SDK。

| 入口 | 文件与解析合同 |
| --- | --- |
| `YmlLoader` | 保留主文件、profile、有序 overlay、环境覆盖；不会自动执行 `yml.imports`。 |
| `strict::ConfigLoader` | 显式严格文档、环境快照、类型绑定、预算和本地 imports 装配。 |
| `ConfigLoader::memory` / `load_documents` | 只使用已取得的文档和环境；不会根据文档名称、imports 或 profile 自行打开文件。 |
| `prepare` / `PreparedLoad::bootstrap` / `finish` | 先解析来源与必要引导依赖，取得远端文本后再求值最终业务树。 |

旧入口的整值环境回退和默认值保留标量推断；严格入口默认保留文本，再按目标字段类型绑定。两者均支持嵌套默认值、真实依赖环检测、字面量保护和失败时不修改输入。旧原地入口保留点号/方括号字面键，点分名称冲突仍按对象遍历顺序采用后遇到的标量。例如 `{"aa":{"bb":"nested"},"aa.bb":"literal","copy":"${aa.bb}"}` 按此顺序遍历时，`copy` 得到 `literal`，原字段均保留。严格入口则拒绝这些含糊键名。非法表达式在两类入口都被拒绝，保留未命中引用不放过非法结构。严格新入口不能通过更换类型名后不经配置迁移就视为完全兼容。

兼容入口也固定一次环境映射，但不套用严格快照的全环境数量或字节限制；无关环境项不会仅因总量使普通配置失败，实际配置及表达式展开仍受预算约束。严格 `EnvironmentSnapshot::capture/from_pairs` 保留独立限额。兼容进程快照忽略不可解码项，严格快照在实际访问非法值时报告编码错误。

| 调用方需要核对的行为 | 兼容入口 | 严格入口 |
| --- | --- | --- |
| 默认值边缘空白 | 求值前裁剪默认源码两侧空白，包含内嵌和嵌套分支 | 保留默认源码空白 |
| 环境与默认文本 | 整值沿用标量推断，内嵌保留文本；空环境值仍命中 | 默认保留 String，绑定到目标字段时受检转换 |
| 键名与冲突 | 保留既有字面键及点分索引覆盖规则 | 拒绝含糊路径和归一化环境覆盖冲突 |
| 文件格式与来源 | 沿用 `YmlLoader` 的兼容范围 | 重复键、非法格式、必需来源缺失和读取变化均拒绝候选 |
| imports | 由调用方显式解析并提供 overlay | 文件加载器执行固定来源计划；内存入口不自动执行 |

完整迁移步骤见[接入与升级](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/migration.md#选择严格配置装配)。

## 严格文件装配

```rust
use naml::strict::ConfigLoader;

#[derive(serde::Deserialize)]
struct Server { host: String, port: u16 }
#[derive(serde::Deserialize)]
struct Settings { server: Server }

let loader = ConfigLoader::standard()?;
let loaded = loader.load()?;
let settings: Settings = loaded.bind()?;
let report = loaded.report();
```

默认主文件为 `zcf/application.yml`。加载器创建时固定环境及绝对路径；同一实例重载不重新捕获进程环境。自定义主路径和 profile 模板分别通过 `base_file`、`profile_pattern` 指定。配置改变使用新候选，不修改上一次结果。

```yaml
application:
  name: notification-service
log:
  path: ${LOG_PATH:/usr/local/logs/${application.name}}
server:
  host: 127.0.0.1
  port: ${PORT:8080}
yml:
  imports:
    - file: common.yml
      optional: false
    - file: /etc/conf/telegram*.yml
      optional: false
    - file: /config/*.yml
      optional: true
```

顺序固定为主文件、活动 profile、按声明顺序展开的导入、环境覆盖。映射深合并，数组整体替换，null 保留为显式值；空映射不清空已有子树。相对导入路径基于主文件目录，不能含 `..`，不会执行 shell、`~` 或变量命令替换。

严格 map 导入必须显式声明布尔 `optional`，file 与 nacos 互斥，未知字段拒绝。字符串形式保留 `file:`、`optional:file:`、`nacos:`、`optional:nacos:` 语义。`optional` 只允许来源缺失：已经取得的空白、坏编码、坏格式、错误文件类型、超限及身份变化均拒绝整轮。

本地 file 声明不接受冒号协议地址；字符串形式先移除 `file:` 标记，再与 map 形式执行相同校验。精确文件和模式的扩展名都在声明阶段校验，只允许 YAML/YML、JSON、TOML；无扩展名按 YAML 处理。`optional: true`、文件不存在或模式零匹配均不会放过不支持的扩展名。

导入文件不能声明 `yml`、`nacos` 或 `secret_providers` 控制段；不递归执行导入。独立本地 `load()` 遇到必需 Nacos 来源会失败，应由 `config-boot::strict` 提供远端文本。

## 通配符与覆盖顺序

只支持确定目录内文件名部分的 `*`、`?`，完整模式路径最多 4096 字节。`?` 匹配一个 Unicode 标量值，`*` 不跨目录；拒绝 `**`、目录通配、字符组、大括号扩展与模式元字符转义。前导点文件只在模式显式以点开头时匹配。`*.yml` 会匹配 `draft.tmp.yml`，因此临时文件应使用不匹配的后缀再 rename。

每条模式独立按完整文件名自然排序：连续 ASCII 数字段比较数值，等值时短数字段在前，其余部分比较原始 UTF-8 字节，不折叠大小写、locale 或 Unicode 组合形式。例如 `config-1.yml`、`config-2.yml`、`config-02.yml`、`config-10.yml` 依次加载，后者覆盖前者。多个模式组与精确文件保持声明位置，不重新全局排序。

必需模式零匹配失败，可选模式零匹配保留目录观察需求。目录枚举遇到非 UTF-8 名称拒绝；匹配到目录、FIFO 或其它非普通文件也拒绝。按真实身份去重，Unix 下包括硬链接；来源链接切换、读取前后身份/内容或匹配集合改变时，不返回旧候选。

`FilePattern::expand_names` 允许具有独立隔离读取者的应用复用同一匹配和排序规则，不必把文件系统读取移回业务线程。

## 表达式、字面量和类型

查找顺序为确切配置路径、原样环境名称、规范化环境名称、默认值。当前字段自引用只跳过树候选。规范化把点号和连字符换成下划线并转 ASCII 大写，因此 `${aa.bb.cc}`、`${aa-bb-cc}`、`${AA_BB_CC}` 都能命中 `AA_BB_CC`；确切树值或原样环境键仍优先。空环境值也属于命中。

- `${LOG_PATH:/usr/local/logs/${application.name}}`：支持默认分支嵌套；环境已设置时不访问默认分支依赖，也不留下多余右括号。
- `${A:${B:}}`：空默认值合法；`${URL:https://example.invalid/a:b}` 的后续冒号是正文。
- `$${name}`：得到字面 `${name}`；环境原文和受保护字符串中的表达式不会再次执行。反斜线没有另设转义语义。
- `${servers[0].host}`：支持数组元素只读引用。拒绝对象、数组、null 在标量位置复制，区分缺失与显式 null。
- key 两侧空白被去除；严格默认文本保留空白。兼容入口在求值前去除默认分支源码两侧空白，整值、内嵌和嵌套分支使用同一规则；引号内空白、引用取得的树值及环境原值不会因此裁剪。
- 嵌套默认分支只有一个引用时沿用该引用已确定的类型；内嵌表达式不会因进入默认分支而启用整值标量推断。
- 未采用默认分支仍必须结构合法，但不触发环境或字段访问；`${${NAME}}` 等动态 key 不支持。

`LoadPolicy.placeholders=false` 原样保留文本；`preserve_unresolved=true` 仅保留结构合法的未命中引用。路径提示 `Literal` 保护子树，较具体路径的 `Resolve` 可以恢复解析。非法语法、循环和超限不会被保留模式吞掉。

严格树 API 中环境覆盖、环境回退与默认文本默认是 String，树标量引用保留原类型。`LoadedConfig::bind`、`bind_at` 和 `strict::bind` 根据目标 `u16`、bool、String 等受检转换：端口 `"9090"` 可绑定 u16，密码 `"001234"` 保持原文。整数越界、非有限浮点与非法布尔文本失败，错误不包含原值。动态 Value、Serde 的无标签中间表示仍保留文本；直接要求原生 JSON 数值的消费者需要原生文档标量或受信 `ValueHint::Scalar`。`String` 提示要求文本，`Json` 提示显式解析有界 JSON 值。

## 内存文档与环境策略

```rust
use naml::{ConfigFormat, strict::{ConfigLoader, EnvironmentSnapshot, SourceDocument}};

let environment = EnvironmentSnapshot::from_pairs([
    ("PASSWORD".to_owned(), "001234".to_owned()),
    ("APP__SERVER__PORT".to_owned(), "9090".to_owned()),
])?;
let document = SourceDocument::new("application", ConfigFormat::Yaml,
    "server:\n  port: ${PORT:8080}\npassword: ${PASSWORD}\n");
let loaded = ConfigLoader::memory(environment).load_documents(&[document])?;
```

`environment_overlay` 与 `environment_fallback` 独立控制 `APP__` 结构覆盖和 `${...}` 回退；`allow_overlay`、`allow_fallback` 分别约束允许名称。快照查询不会回到真实环境，非法编码值在被访问时拒绝。空前缀分隔符表示单下划线前缀和平面键。

默认环境值是文本；`structured_environment=true` 显式允许 JSON 对象/数组及数字索引。覆盖现有数组元素必须在范围内，不创建稀疏数组；同层重复、大小写归一碰撞和父子路径冲突均拒绝。对象数字键与数组索引是不同路径身份。

## 文档与预算

严格入口接受 YAML/YML、JSON、TOML，一来源一映射文档，要求 UTF-8，可有 BOM。显式 `{}` 合法；仅注释、空白、null/数组根、多文档流、重复键和非字符串 YAML 键拒绝。YAML 支持普通锚点及有界别名复制，拒绝 merge key 与未知 tag。引号保护的 `"<<"` 是普通键。严格键名不接受点号、方括号、空键或控制字符，避免路径歧义；使用嵌套映射表达层级。

`ProfileSelection` 可指定名称、使用环境或显式关闭。严格 profile 默认缺失报错、多个候选报歧义；依次探测精确路径及 toml/json/yaml/yml，策略可显式放宽。名称拒绝路径分隔符、`..` 和控制字符。无扩展名严格文件按 YAML 解析。

默认 `LoadLimits`：单来源 4 MiB，总输入 32 MiB，声明 128，模式 64，来源 128，每模式匹配 64，目录枚举 4096，深度 64，节点 100000，单文本 1 MiB，累计表达式展开 16 MiB，输出及解析文本 32 MiB，依赖访问 200000，观察目录 256。别名复制、被覆盖来源及环境都计入相应预算。环境快照另外限制 4096 项、2 MiB。加载器策略来自调用方，导入文档不能提高限制。

读取、解析和求值在可检查边界响应取消标记；同步库不能硬终止内核文件系统等待。需要硬期限的宿主应使用可终止读取进程，再通过内存文档入口装配。`allowed_roots` 限制符号链接解析后的真实来源与观察祖先，但不提供针对恶意本地重命名竞争的文件系统沙箱。

## 来源解释与观察

`LoadedConfig` 提供有序 `sources`（类别、声明索引、组内次序）、字段 `origins` 覆盖记录、字段依赖、环境访问关系、环境覆盖位置与确切变量名 `environment_origins`，以及 `report()` 规模摘要。绑定错误包含字段路径；解析器可用时提供来源 ID 与行列；依赖环保留闭环路径。默认 Debug/Display 不回显正文、环境值、来源路径或摘要。显式访问名称、路径、环境名与指纹时，调用方负责展示权限，不应直接写公开日志或指标。

值 `fingerprint()` 用于候选值去重；`source_fingerprint()` 包含有序文档身份和观察目标，`watch.fingerprint()` 包含来源身份、匹配集合、缺失候选和观察目标。两者必须分开，即使值未变化也要对账新来源。

```rust
use naml::strict::{ConfigLoader, ConfigWatcher};
let loader = ConfigLoader::standard()?;
let loaded = loader.load()?;
let mut watcher = ConfigWatcher::new(&loaded.watch, loader.load_policy().clone(), |_| {
    // 只向宿主发合并唤醒信号，重读和业务发布在宿主任务中执行。
})?;
let next = loader.load()?;
watcher.reconcile(&next.watch)?;
let health = watcher.health();
```

回调使用预计算别名和相同文件名匹配器，不执行文件读取或 canonicalize。缺失目录观察最近可用祖先，逐级出现后重建观察。事件不保证恰好一次；`Rescan`、`BackendError` 和健康状态要求宿主补读或重建。新目录全部挂载后才切换目标，旧观察撤销失败保留并记录，后续对账继续清退。宿主仍需要周期补读；后端故障后应重建 watcher。

## Application 与配置中心

```rust,ignore
/// 业务作用：在 preflight 前固定配置权限和环境。
/// 参数说明：无。
/// 返回：标准严格来源加载器。
fn configuration() -> nasa::yml::strict::Result<nasa::yml::strict::ConfigLoader> {
    nasa::yml::strict::ConfigLoader::standard()
}

/// 业务作用：将业务初始化登记到统一生命周期。
/// 参数说明：`app` 是受管应用。
/// 返回：登记成功后继续启动。
#[nasa::application("log", "web", config = configuration)]
async fn main(app: nasa::Application) -> anyhow::Result<()> { Ok(()) }
```

手动入口使用 `ApplicationSpec::with_config_loader(configuration)`，与宏共享 preflight。不指定工厂时保留兼容文件读取范围。`config-boot::strict::prepare/load/assemble` 将 `nacos.imports` 放在 `yml.imports` 前，校验归一后的远端身份、必需文档、顺序与格式；首拉前只求值必要引导依赖。

宿主冻结主路径、profile、环境、imports、模式、连接与信任根。运行期只允许固定模式的匹配集合和业务正文变化；来源权限变化拒绝候选并要求重启。启用 `application,yml-watch` 和 `config_watch.enabled: true` 后，本地事件与 15 秒周期补读进入同一候选流程，Batch 拒绝持续监听。来源失败保留旧视图和观察；值相同仍更新来源观察。视图发布与全部组件应用成功不同，组件分别记录 Applied、ApplyFailed、RestartRequired，不承诺跨组件统一回滚。

`app.config_observation()` 提供独立的来源观察序号和规模摘要。候选拒绝时不推进；相同值的来源重命名或远端正文变化仍可推进观察序号，不伪装成业务配置版本。

组件状态为 `ApplyFailed` 时，同值补读可能被值指纹去重，并不保证再次安装失败目标。确认资源条件恢复后应重启应用重新装配；`RestartRequired` 同样通过重启生效。来源解析失败则保留当前视图，并继续事件观察与周期补读。
