# Outbox 数据库迁移

生产数据库不能用运行期 `ensure_schema` 替代结构治理：

1. 新部署执行 `outbox_schema.up.sql`，直接建立当前完整结构；
2. 已有 `(dispatched, id)` 旧索引的部署执行 `outbox_dispatch_indexes.up.sql`，在线替换索引；
3. 需要多通道分片的部署执行 `outbox_channel_lanes.up.sql`：加 `channel` 列（历史行一次性归入
   `'global'` 默认 lane，归属规则不可再改）与 `(channel, dispatched, dead, id)` 领取索引。
   本步不改变行为，可先于任何 dispatcher 变更独立上线并回退；启用按 lane dispatcher 后，
   同库**禁止**再运行未分片 dispatcher（两种 claim 锁名不同，并行会双重发布）。
4. **所有部署**在滚动新 binary 之前执行 `outbox_event_tenant.up.sql`：加 `tenant` 列
   （历史行一次性归入 `'system'` 租户）与 `(tenant, dispatched, dead, id)` 对账/观测索引。
   这一步不是可选项——新 binary 的 append 路径无条件写该列，列缺失会让业务写入全部失败。
   本步不改变行为，可先于任何 binary 变更独立上线并回退。租户身份只能由已认证业务上下文
   经受信入口写入，不得从 payload 解析。
5. 只有**启用**每租户在飞事件配额的部署才需要 `outbox_tenant_quota.up.sql`。未启用配额的
   部署既不读也不写该表，投递与死信裁决路径不引用它。启用方必须在开启配额前执行本步：
   受管组件在 Ready 前校验列与表齐备，缺失即拒绝启动，不会拖到第一轮投递才暴露。
   **把某租户纳入配额前，该租户全部写入必须已改走受信上下文入口**，否则旧路径写入的行会在
   释放时造成账本漂移（由 `reconcile_outbox_tenant_quota` 在事务内有界对账收敛）。
   账本带初始化标记，仅事务内对账置位：启用上限前必须先在受控窗口对该租户执行一次对账,
   把存量待投递行入账；未置位时受信 append 与受管 Ready 校验都按部署错误拒绝——存量行的
   投递/死信释放会扣掉新行名额，上限被静默穿透。零上限（封禁）租户同样需要初始化。
6. 启用保留清理（retention）的部署执行 `outbox_dead_lifecycle.up.sql`：加 `dead_at` 生命周期
   列、两条保留清理索引与 `outbox_dead_disposal` 处置事实表。历史死信行 `dead_at` 为 `NULL`
   时永不进入清理候选，须由运维核对后显式回填。
7. **仅存量窄宽度部署**（早期版本迁移建出的 `tenant`/`tenant_id` 为 `VARCHAR(190)`）执行
   `outbox_tenant_width.up.sql`，把两列对齐公开租户身份合同的 256 字节。新建库与直接执行
   当前版本第 4/5 步的库已是 256，无需重复。列宽不足时 191..=256 字节的合法租户能通过全部
   身份校验，却在首笔 Outbox 写入或账本操作处失败；启用配额的受管部署会在 Ready 结构校验
   处按部署错误拒绝并指向本迁移。
8. 部署系统保存执行事实，禁止对已经具备当前索引的新表重复执行历史索引迁移。

当前结构优化三条固定查询路径：

- `(dispatched, dead, id)` 为待投递计数提供覆盖读取，并为按 `id` 有序批量领取定位真实候选；领取
  payload 等事件列时仍按主键回表；
- `(dead, id)` 服务死信计数；
- 查询前缀已由新索引包含的 `(dispatched, id)` 在新索引建立后删除，避免重复写放大。

DDL 前先在目标规模副本上核对执行计划、metadata lock、复制延迟、索引空间和完成时间。生产表持续写入时，
必须使用组织批准的在线 DDL 执行器保持等价索引语义。部署记录应保存目标逻辑库、起止时间、影响行数、
索引定义和脱敏错误，不得输出 payload、凭据或完整业务身份。

`outbox_dispatch_indexes.down.sql` 只用于 binary 回退已经明确要求旧索引形态的场景。回退会重新引入
死信计数全表扫描和死信热区过滤成本，执行前必须停止指标抓取与 dispatcher，并取得数据库负责人批准。
