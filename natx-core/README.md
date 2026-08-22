# natx-core

`natx-core` 为 MySQL 与 PostgreSQL 事务运行时提供唯一的 datasource 名称、driver、Application owner
和进程模式裁决。它不依赖 SQLx 的数据库 feature，也不创建连接池。

业务通常通过 `nasa`、`natx` 或 `natx-pgsql` 使用这些能力，不直接操作进程协调入口。受管模式先发布
关闭态 catalog，待两类 typed registry 都完成资源登记后再开放；开放前所有全局 getter 都返回不可用，
不会观察到半张资源表。

## Catalog 与所有权架构

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
