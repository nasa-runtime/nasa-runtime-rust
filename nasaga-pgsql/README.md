# nasaga-pgsql

`nasaga-pgsql` 是 NASA Saga 的 PostgreSQL 持久后端，实现 `nasaga-backend`
分组能力，与 `nainbox-pgsql`、`naoutbox-pgsql` 和 `natx-pgsql` 共享同一
datasource 与 ambient transaction。

它保存 Saga 实例、attempt、transition、参与方 gate、durable timer、租户治理和
审计事实。状态裁决不在本 crate 复制，由 `nasaga-runtime-core` 统一执行。

## 事务与并发边界

- 除 timer 领取与交还外，所有写入要求已存在同 datasource PostgreSQL 事务。
- timer 租约使用 owner、过期时刻与不可复制 fencing token；失权 worker 的条件写会被拒绝。
- 并发 timer 领取使用 `FOR UPDATE SKIP LOCKED`，不依赖 session advisory lock。
- SQLSTATE `23505` 只在已声明的业务唯一约束处转换为幂等或冲突；其它约束失败保留为基础设施错误。
- 本后端不提供跨 datasource、跨 driver 或 XA 事务。

创建幂等由 `(tenant_id, workflow_name, business_key)` 唯一事实与请求摘要共同裁决。
请求 `saga_id` 已被其它业务意图占用时，即使业务键命中另一实例也返回冲突。两种身份在原事务内
锁定核验；未占用的新 `saga_id` 可按相同业务键和请求摘要取得原实例的重复收据，不追加创建副作用。

## 初始化与配置

```toml
[dependencies]
nasaga-pgsql = "2.0.0"
natx-pgsql = "2.0.0"
```

```rust
let store = nasaga_pgsql::PgSagaStore::with_datasource("orders")?;
```

schema 由 Application 在角色 Ready 前确保：Orchestrator 结构包含全局状态、timer、Catalog、
capability、审计与配额，参与方结构只包含 gate；Inbox 与 Outbox 由对应 adapter 确保。
`PgSagaStore::ensure_schema` 与 `PgSagaStore::ensure_participant_schema` 使用组件内嵌结构，
仅用于 standalone 引导，不会创建连接池或改变 datasource 所有权。
统一审计表尚不存在时可按当前合同首次创建并回填；一旦审计表存在，列、主键、逻辑唯一约束、stream
索引、函数与 trigger 必须在回填前匹配，回填提交后还会再次复验。任何未知漂移都会阻止 Ready。

审计读取基于 `saga_audit_event` 的全局只增序号。attempt 创建及后续状态变化、实例迁移、控制操作、
管理操作与冲突事实都在原业务事务内追加独立事件，因此持久游标不会因类别切换、字典序或原地状态变化
漏过后提交的事实。trigger 在分配序号前递增按 Saga 隔离的 `saga_audit_stream_guard`，使同一实例的
后发事务必须等待先发事务提交，同时不阻塞其它 Saga。

本 crate 不读取 YAML。业务经门面使用时开启 `saga-runtime-pgsql`，从 `nasa::saga::pgsql` 使用运行时
包装；Application 依据当前角色作用域内的 `datasource_ref` 复验 driver，并拥有 pool、Catalog、timer
与停机顺序。managed 模式下业务不提交 Saga 运行计划。

## 参与方 resolve 的事实边界

外部效果仍为 `UNKNOWN` 时，gate 决定 execute/compensate 查询目标；`HALTED` 只冻结解决通道，
不抹去原未知事实，重新查询必须携带受信恢复操作。已完成的 resolution 对新 attempt 重放原裁决。

若正向效果已为 `SUCCEEDED/REJECTED`，尚未开始补偿且 resolution 为 `NONE`，result 回传未知不等于
业务效果未知：首个 resolve 在同一事务中记录对应 resolution 终态和 resolve effect，runtime 为当前 command
写入结果 Outbox，不调用业务 resolver。缺少原效果或目标方向不明确时仍冻结，不凭空查询或推测成功。

## 观测

`PgSagaStore::load_operational_metrics(now_ms)` 从已提交表聚合实例终态、Unknown、人工介入、重试、冲突、
当前运行状态、到期 timer 与生命周期耗时。结果只含低基数总量，不使用 saga id、tenant、step 或
datasource endpoint 作为 label；数据库查询失败保持失败，不能用进程内计数伪装持久事实。
