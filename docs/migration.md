# 接入与升级

[中文](migration.md) | [English](migration.en.md)

本文说明现有 Rust 应用如何采用当前 `nasa 2.0.3` 的依赖与运行合同。步骤取决于应用使用的能力，
不能仅凭版本号推断 API、数据库结构、消息协议或配置兼容。最小新应用见 [快速开始](quickstart.md)。

## 固定依赖来源与能力

业务优先通过门面选择 feature：

```toml
[dependencies]
nasa = { version = "2.0.3", default-features = false, features = ["application", "web"] }
```

`version = "2.0.3"` 是 Cargo 的兼容范围约束，应用的 `Cargo.lock` 固定实际解析结果。采用线上组件时，
移除指向本地 NASA 源码的 `path` 与 `[patch.crates-io]` 覆盖，保留原本需要的 feature。
直接使用 `sqlx::FromRow` 或 `sqlx::migrate!` 的业务仍需声明对应直接依赖；门面不替业务隐式提供外部 crate 名称。

检查应用依赖图，确认同一组件没有 registry、Git 和本地路径混合身份：

```bash
cargo metadata --format-version 1
cargo tree -d
cargo check --all-targets --locked
```

按应用实际支持的 feature 组合构建。依赖图中的无关第三方重复版本不一定有问题；需要重点核对同一
事务上下文、宏 ABI、协议类型与资源 owner 是否来自同一组件身份。不要为升级一个组件无关更新整张依赖图。

## 移交生命周期所有权

| 业务现状 | 采用 Application 时的处理 |
| --- | --- |
| 自行创建 Tokio 入口和信号循环 | 使用 `#[nasa::application]` 作为唯一进程入口 |
| 自行创建数据库、Redis 或 Kafka 连接 | 将来源放入最终 YAML，由对应组件管理 |
| 手写 listener 或后台消费循环 | 使用受管入口；自管 listener 经 `serve_when_ready` 放行 |
| 启动时需要业务资源 | 在 startup Hook 登记计划，在 initializer 取得 Prepare 后资源 |
| 自有资源需要异步收尾 | 在 Hook 登记 `register_graceful_shutdown`，或移交给资源关闭 owner，避免重复关闭 |

不需要 Application 的库仍可显式装配组件，并自行承担门禁、观测和关闭责任。先决定每个资源的唯一
owner，再调整业务接线。直接复制外部客户端句柄不能获得与受管借用相同的撤销保证。

## 核对配置与入口

- `zcf/application.yml` 必须存在，按进程工作目录解析。
- `application.mode` 决定 Service 或 Batch；无入站的常驻后台服务应显式使用 `service`。
- 单源数据库使用 `database`，多源使用 `datasources`，两种根互斥；PostgreSQL 显式声明 `driver: postgresql`。
- datasource、Redis qualifier 和 Kafka client name 都是资源身份；显式引用不存在时拒绝启动。
- 单源 Redis 的持久身份为 `primary`，`default` 是查询别名，不用于更改持久 key 空间。
- 使用 `/readyz` 判断接流，不能只判断端口或某个组件启动成功。
- 连接来源与其它冻结字段变化需要重启；读取新配置不证明热应用完成。

字段、默认值和准入条件以 [Application 合同](../napp/README.md) 为准。

## Kafka 源码与配置迁移

`nafka 2.0.3` 及其 `napp`、`nasa` 入口包含源码不兼容变化，使用方必须迁移并重新编译，不能依据补丁号假定直接替换兼容。尚未完成迁移的应用应通过原锁文件固定依赖，避免无审阅地更新。

| 调用方用法 | 需要采取的动作 |
| --- | --- |
| 完整字段构造 `ConsumerConfig` | 补充 `concurrency`、`group_concurrency`；优先使用 `ConsumerConfig { concurrency: 2, ..Default::default() }`，并核对其余默认值 |
| 构造 producer lane 等公开配置结构 | 按当前结构补齐新增字段；使用 `Default` 的配置也需核对可靠性与容量策略 |
| 穷尽匹配 `NafkaError` 或完整字段模式 | 覆盖新增错误类别及字段；区分确定失败、结果未知、权限拒绝和关闭未完成，不把未知结果作为普通可重试失败 |
| 依赖 producer 默认值 | 当前默认 `acks=all`、`enable_idempotence=true`，队列上限为 10000 条和 65536 KiB；根据 broker ISR、吞吐和内存预算显式选择 |
| 通过受管 Admin 修改资源 | 显式配置 `container.admin_write` 并取得 broker ACL；默认只读入口不授予写权限 |
| 旧配置携带未识别字段或原生透传键 | 使用 `inspect_legacy_config` 核对转换候选和人工决策项，按当前严格配置合同调整后再启动 |

不自动迁移 group 位点、事务身份或 checkpoint。迁移后的启动成功也不证明旧持久任务可以按新计划接续。

## Kafka 身份与交付合同

普通组与事务组共同支持1至1024个 UTF-8 字节、无NUL、非共同纯空白且首尾不含U+0000–0020的实际名称；合法原文不裁剪、不归一化。宏省略group才表示沿用默认，显式空字符串不是实际组。旧组超出支持域时必须使用可处理原身份的客户端或显式规划迁移，不能自动截短或复制 committed offset。

配置转换使用 `nasa::kafka::inspect_legacy_config` 生成候选及人工决策项，不修改原配置或broker。按来源核对发布可靠性、consumer并发、输入预算、错误策略、Admin写权限和Ready条件；组、资源名、广播身份或事务部署身份的变更都可能改变持久责任。

Java/Rust互通须保持event、key、tombstone、有序header及编码含义。有限任务接续还须复验完整计划、物理资源和checkpoint；Kafka事务只覆盖同集群完整输入位点与批准输出，普通lane和外部数据库写入不自动加入。详见 [共同合同](../nafka/README.md#java-与-rust-的共同合同)。

## 接入多注册中心

单 Nacos 的 `nacos-discovery` 组件与 `rest_discovery` 根继续可用。采用多连接时，启用所需
`discovery-nacos`、`discovery-consul`、`discovery-kubernetes` feature，组件改为 `discovery`，
删除旧根并配置 `discovery.connections`；新旧根不能并存，即使其中一个 disabled。
Nacos 的受管 HTTP adapter 要求 Nacos 3，不能把 SDK 的服务端兼容范围套用到此入口。

- 默认连接直接命名为小写 `primary`；其他连接通过 `qualifier` 精确选择。
- 注册清单与 Saga 目标使用 `qualifier`；`connection_ref` 仅为兼容别名，不能同时声明。
- 旧 `default_connection` 保留显式默认引用语义且优先于 `primary`；新配置无需此字段。
- Mapping 使用 `Client::with_clients(app.discovery()?.rest_clients()?)` 注入集合；方法值覆盖 trait。
  `Client::new(selected_client)` 继续支持没有 qualifier 的接口，不自动建立具名身份。
- 对互联网 API 使用完整 `url`，不配置或继承 qualifier；动态地址使用运行期请求构建器。
- 每个注册目标分别配置外部可达 IP 与映射端口；增加连接不会自动发布自身。

连接配置冻结在 owner 周期内，变更后重启；napp 对 `discovery` 变化拒绝整份热更新候选。
完整 YAML、内部接口与 Google API 代码见[调用指南](discovery.md)。


## 选择严格配置装配

`YmlLoader` 保留既有主文件、profile、overlay 和环境处理范围，不自动执行 `yml.imports`。
需要有序通配符导入及来源解释时，显式选择 `nasa::yml::strict::ConfigLoader`；Application 使用
`#[nasa::application(..., config = configuration)]`，工厂在 preflight 前返回严格加载器。
不能等到业务 Hook 才设置来源策略。示例见[门面配置入口](../nasa/README.md#严格配置装配)。

| 核对项 | 调用方需要采取的动作 |
| --- | --- |
| 文件与声明 | 严格 map import 显式设置布尔 `optional`；确认主路径、profile、导入目录和允许根，删除文档重复键及含糊字面键 |
| 合并顺序 | 主文件 → profile → 有序 imports → 环境覆盖；配置中心适配器把 `nacos.imports` 放在 `yml.imports` 前 |
| 通配符覆盖 | 每组按完整文件名自然排序，数字 `2`、`02`、`10` 依次加载；后文件覆盖前文件，不跨组重排 |
| 类型 | 严格环境覆盖、环境回退和默认文本先保留 String；通过 `bind`、`ConfigSnapshot::deserialize/section` 绑定目标类型，原生 JSON 消费者显式提供文档标量或受信 `ValueHint::Scalar` |
| 默认空白 | 兼容入口在求值前裁剪默认源码两侧空白，严格入口保留；引用取得的值不受这项裁剪影响 |
| 环境别名 | `${aa.bb.cc}`、`${aa-bb-cc}`、`${AA_BB_CC}` 仍可回退到 `AA_BB_CC`；确切树值和原样环境名优先，空值仍命中 |
| 嵌套和字面量 | `${LOG_PATH:/usr/local/logs/${application.name}}` 支持嵌套默认；字面 `${name}` 写作 `$${name}`，环境正文不递归执行 |
| 失败语义 | 严格模式拒绝坏格式、非法结构、路径冲突、必需来源缺失、来源变化与超限；保留未命中模式不能放过非法表达式 |
| 运行期变化 | 固定环境、profile、imports、模式、连接与信任根需要重启；仅在已有模式内增删文件不改变权限 |

纯内存 `load_documents` 不执行文档内 imports；硬超时读取进程可以把取得的文档交给它，再由宿主
校验和发布。严格文件入口不能作为恶意本地文件系统操作的隔离沙箱，也不能硬终止内核等待。

热更新同时检查来源观察序号、配置快照版本和组件状态。`ApplyFailed` 不因无关或同值重读自动清除，
同值候选可能被去重；资源条件恢复后可重启重新装配。详见[运维指南](operations.md#配置刷新)。

## 选择 YAML 文档入口

应用配置继续使用 `nasa::yml::YmlLoader` 或显式严格加载器；外部协议文档使用
`nasa::yml::yaml::{from_slice, to_string}`，两者均由 `yml` feature 提供。组件内部使用 `naml`
的对应入口，无需业务另选解析后端。自定义预算使用 `from_slice_with_limits`、
`to_string_with_limits` 和 `Limits`；输入及输出的中性值类型为 `serde_json::Value`。

接入文档读写时，确认来源是单个 UTF-8 文档，键为字符串；删除重复键，将 merge key 展开为
明确字段，移除未知标签。普通锚点和别名在预算内可用。整数须在 `i64`/`u64` 范围，小数须为有限
`f64`；需要原始拼写或超出数值范围的业务标识应显式写成字符串。编码不保留注释、排版或锚点名称。

外部文档中的点号、方括号和空字符串键按原文保留，`${...}` 与 imports 不会执行；不能据此推断
严格配置也接受这些键。调用方负责将返回的完整值交给协议校验，并在文件替换或外部 reload 前
完成权限检查。示例与预算见 [门面文档入口](../nasa/README.md#外部-yaml-文档)；kubeconfig 的更小
限额及凭据规则见 [Kubernetes 认证](../nadisc-kubernetes/README.md#kubeconfig-文档边界)。

## 核对持久状态与重试

升级二进制前应确认运行中的 schema、消息保留窗口、Saga 定义、身份和已有业务实例能被目标配置读取。
业务 SQL 迁移由应用显式登记；不要把换 crate 版本当作自动迁移既有数据的许可。

| 能力 | 必须保留的业务合同 |
| --- | --- |
| 事务、Inbox、Outbox | 同一原子链保持相同 driver 与 datasource |
| 可靠 Saga client | 业务事实、start-intent 与 dispatcher 使用同一事务域 |
| Saga 恢复 | 保留实例引用的定义与摘要；未知结果经 resolve 或人工裁决，不盲目重试 |
| Redis 消费 | 保留业务键路由、group 身份与 PEL 责任，不把 ACK 不明视为未执行 |
| Inbox 保留 | 去重标记保留窗口覆盖实际最大重投视界 |
| 停机 | 先关闭新准入，再等待在途责任，最后释放依赖 |

先为目标环境确认备份与恢复路径。应用回退必须同时考虑数据与协议的可读性，不能承诺仅回退二进制
即可撤销已完成的 schema 或业务写入。详见 [Saga 生产指南](saga-production.md) 与 [部署指南](deployment.md)。

## 兼容性信息

当前接口、能力边界和配置以组件 README、rustdoc 与协议文件为准。升级涉及公开 API、配置形态或
持久语义时，中文与英文接入说明应同步维护，并说明调用方需要承担的变化。源码沿革保留在 Git 中。
本文不宣称所有旧版本都能直接升级；使用独立组件的应用还需核对该组件的装配合同。
