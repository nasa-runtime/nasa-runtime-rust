# natx-core

`natx-core` 为 MySQL 与 PostgreSQL 事务运行时提供唯一的 datasource 名称、driver、Application owner
和进程模式裁决，并提供连接等待、拒绝门禁与 Pool 状态的统一观测合同。它不依赖 SQLx 的数据库
feature，也不创建连接池。

业务通常通过 `nasa`、`natx` 或 `natx-pgsql` 使用这些能力，不直接操作进程协调入口。受管模式先发布
关闭态 catalog，待两类 typed registry 都完成资源登记后再开放；开放前所有全局 getter 都返回不可用，
不会观察到半张资源表。

## 连接观测合同

连接获取按 `driver/datasource/purpose` 固定身份记录；`purpose` 区分 `mapper`、`migration`、
`probe` 和 `direct`。这些是等待与连接事实，不是 Mapper SQL 执行时长，迁移锁等待也不计入 Pool
acquire。框架不能从 `PoolTimedOut` 推断是池饱和还是握手失败。

`observability` 为启动期 datasource 目录预分配原子计数、in-flight 和固定桶 Histogram。
完成路径只更新对应单元；未知 datasource 共用固定回退槽，不根据请求内容扩张指标目录。
Pool acquire 与 transaction slot wait 分开计量，取消由生命周期守卫收口且不会重复计数。
事务门禁、跨 datasource、跨 driver 和未知名称作为执行前拒绝，不归入数据库调用失败。

`ConnectionMetricsSource` 与 `PoolMetricsSource` 在出口侧构造结构化样本，并提供最坏系列预算。
Pool 状态允许并发变化，使用数采用饱和减法；全量指标不受语句日志开关影响。
默认等待日志关闭，显式启用的连接超时通知只能经具体的有界队列非阻塞入队，不能在热路径执行渠道
回调。路由、日志策略和指标槽按进程生命周期保留，重启后重新安装。
等待诊断在原子终态记录之后执行，日志 subscriber 的 unwind 不改变连接结果；取消析构不输出日志。

## Catalog 安全边界

单个 ambient transaction 只能绑定一种 driver 和一个 datasource。跨 driver 的耐久业务写需要使用
Outbox/Inbox 收敛，不能把两个本地事务解释为一个原子事务。

`DatabaseCapabilities` 表示当前产物实际编入的 typed runtime，与始终可解析的 `DatabaseDriver` 分离。
配置声明未知能力时应在建连前拒绝，不能把缺失后端误报为 URL 错误。

## 事务裁决边界

`DatasourceRef::new` 保留既有 `anyhow::Result` 签名，供 MySQL adapter 无需改动地继续使用；需要按名称
原因分支的新配置与 checked 入口使用 `DatasourceRef::try_new` 获取 `DatasourceNameError`。

受管发布顺序固定为：构造关闭态 `DataSourceCatalog`，安装同 owner 的 typed weak registry，绑定 catalog，
提交完整 catalog，开放 typed registry，最后开放 catalog。typed registry 即使已经进入 accepting 状态，
在 catalog 开放前也不能发放 pool。停机按相反方向先封口 catalog，再按 owner 与 Arc 身份撤销 typed
registry，旧实例不能清除新实例资源。

`TxEntryError` 为加法 checked 入口分别保留 `DataSourceLookupError` 与 `TxRunError`；已有事务入口可以继续
把 lookup 映射为稳定、脱敏的基础设施原因。driver task-local 只覆盖事务业务闭包，COMMIT 和
after-commit 位于 scope 之外。
