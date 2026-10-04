# 接入与升级

[中文](migration.md) | [English](migration.en.md)

本文说明现有 Rust 应用如何采用当前 `nasa 2.0.1` 的依赖与运行合同。步骤取决于应用使用的能力，
不能仅凭版本号推断 API、数据库结构、消息协议或配置兼容。最小新应用见 [快速开始](quickstart.md)。

## 固定依赖来源与能力

业务优先通过门面选择 feature：

```toml
[dependencies]
nasa = { version = "2.0.1", default-features = false, features = ["application", "web"] }
```

`version = "2.0.1"` 是 Cargo 的兼容范围约束，应用的 `Cargo.lock` 固定实际解析结果。采用线上组件时，
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
