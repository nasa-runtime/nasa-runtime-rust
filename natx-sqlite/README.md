# natx-sqlite

为本地 SQLite 文件提供受管数据源、受控 SQL 执行、同源事务、checkpoint 和一致性备份。数据库文件的独占权随真实原生连接存活；调用取消或 Application 停机超时不会提前释放文件锁。

业务通过 `nasa::mapper::sqlite` 沿用 `#[Mapper]`、`#[Query]` 等 SQL 宏；napp 的同一个 `"db"` 组件负责资源创建、迁移门禁、观测和关闭。每个 datasource 的 driver 独立选择，SQLite 不要求同时引入 MySQL/PostgreSQL runtime。

## 运行架构与文件权威

```text
Application 或 standalone owner → 私有文件身份与稳定 owner 锁
  → 有界连接池 → 本次绝对期限与 lease 容量交接 → 原生 worker
  → prepare 权限检查 → 执行 / 同源事务收口 → 受控结果交付
  → 归还可复用连接，或等待真实原生关闭后回收容量
```

业务句柄可以克隆，关闭 owner 保持唯一。连接和 worker 继续保活文件权威，不能把 async 等待结束视为原生访问已经结束。独立租约接纳与空闲关闭决定互斥；环境事务固定物理借用身份。任何失效旧句柄都不能借同名新 owner 恢复执行，具体预算、提交和清理边界见下文。

## 数据源与生命周期

优先通过 `nasa` 的 `application + tx-sqlite` 或 `application + mapper-sqlite` 启用 `"db"` 组件。每个 `datasources.<name>` 独立声明 `driver: sqlite`，可以与 MySQL、PostgreSQL 同时使用。业务通过 `app.sqlite_datasource(name).await` 获取使用句柄，关闭责任由 Application 持有。

```yaml
datasources:
  local:
    driver: sqlite
    sqlite:
      path: ./data/local.db
      create_if_missing: true
      create_parent_directories: true
      journal_mode: wal
      synchronous: full
      foreign_keys: true
      busy_timeout_ms: 1000
    max_connections: 4
    min_connections: 1
    acquire_timeout_ms: 3000
    connect_timeout_ms: 3000
    operation_timeout_ms: 30000
    transaction_timeout_ms: 30000
    stream_idle_timeout_ms: 10000
    max_result_rows: 10000
    max_result_bytes: 16777216
```

`path` 必填，禁止 URI、内存数据库、符号链接主文件及硬链接别名。主文件、锁文件和 sidecar 必须属于当前账号且无 group/other 权限；父目录不能由其它账号写入。相对路径以启动工作目录解析。只有首次启动可以按显式选项创建缺失文件，重连不创建替代空库。稳定 `.owner.lock` 不随退出删除。

只支持 Linux/macOS 的本地文件系统。不要把文件放在 NFS、SMB、多主共享卷，或用其它进程绕过 owner 协议写入。默认 bundled SQLite，运行时要求引擎至少 3.51.3；SQLx 依赖线为 0.9。未提供 Windows、SQLCipher、透明加密或网络文件系统支持。

原生依赖使用 `libsqlite3-sys 0.37`，同一程序只能链接一套 SQLite。同步驱动也必须允许这一原生依赖线；要求 `libsqlite3-sys 0.35` 的 `rusqlite 0.37` 不能与本组件联合解析。不要通过降低原生版本绕过持久化基线，已有同步存储应单独处理依赖兼容。

已有文件的 `journal_mode` 必须匹配声明；普通建连不隐式转换。默认 WAL 使用 FULL，DELETE 默认使用 EXTRA，外键必须开启；可显式选择 NORMAL、FULL 或 EXTRA。NORMAL 和 DELETE+FULL 的断电持久性弱于对应默认策略。macOS 同时启用 `fullfsync` 与 `checkpoint_fullfsync`，备份主文件发布前执行 F_FULLFSYNC，目录发布执行 fsync；Linux 使用 SQLite 同步及文件/目录 fsync。保证依赖文件系统和设备履行同步。`read_only: true` 禁止创建、写 SQL、Immediate 事务、迁移 apply 与 checkpoint，允许 validate 和作为备份源。只读访问仍可能使用 owner 锁或 SQLite 协调文件。

每条连接发布前回读同步、外键、隔离及可信 schema 策略，确认实际 JSON/RETURNING 与绑定容量。`source.capabilities()` 返回真实版本和已确认策略；后续连接必须一致。业务绑定同时服从物理连接的实际变量上限。

数据源层 `min_free_bytes` 默认 67108864，`max_wal_bytes` 默认 268435456；每次执行及健康探测采集状态，空间不足或 WAL 达到阈值时拒绝新的写语句，读取和维护保留。checkpoint 后压力解除可再次写入。这是执行前压力门禁，不是单笔事务的磁盘配额，也不能保证写入过程中空间不耗尽；FULL/IOERR 仍须按实际事务结果处理。

独立使用时调用 `open_owned(name, config).await`，保存 `SqliteDataSourceOwner`，通过 `owner.datasource()` 克隆使用句柄。`try_init_datasource(source)` 可安装 standalone 名称；不能与 Application 受管 registry 混装。结束时调用 `owner.close(deadline).await`。旧 source 或 Mapper client 不能借同名新实例恢复工作。

## 受控执行与事务

`conn_for(name)`、`conn_from(&source)` 返回 `SqliteConn`。SQLx 查询使用 `connection.executor(false)` 读合同或 `connection.executor(true)` 显式写合同；不公开裸 `SqliteConnection`、`Deref`、任意 PRAGMA 或持久 statement。

业务执行、只读预验证与流的每次轮询都会核对当前 driver、source/owner 和物理事务借用身份。事务开始前借出的独立连接不能在事务体中使用，即使来自同一个 source；应在事务体内重新取得连接。事务连接不能转交给没有原事务上下文的子任务，延迟轮询的 future 与已经开始消费的流遵守相同约束。

业务 SQL 必须是单条语句且绑定数量准确。原生 authorizer 在 prepare 前拒绝手工事务、SAVEPOINT、ATTACH/DETACH、TEMP/虚拟表、危险函数、PRAGMA 与迁移历史写入。只读 executor 拒绝写入，包括 CTE/RETURNING 等不能靠前缀识别的语句。`fetch_optional` 也会耗尽结果，确保写 RETURNING 完成后才返回成功。

`run` / `run_for` 使用 Deferred；`run_for_mode` 或 `#[transactional(datasource = "local", mode = "immediate")]` 可显式选择 Immediate。显式 owned 入口使用 `run_decided_from(&source, mode, body)`，不依赖全局名称。嵌套调用只加入同 owner、同 datasource 事务；未指定模式时继承，显式冲突在执行体前拒绝。跨 driver、跨库不提供原子提交。已经进入的内层执行发生回滚、panic 或取消时将外层置为 rollback-only，即使业务捕获错误或超时也不能提交；尚未轮询的内层 future 被丢弃不改变外层状态。

`run_decided*` 使用 `TxDecision` 并区分提交拒绝、提交未确认、回滚失败。只有物理确认提交、结束事务上下文并释放事务连接借用后才执行 `after_commit`；回调通过普通入口重新取连接，可用于单连接池读取已提交事实。事务不会自动重放。task-local 不跨 `spawn` 传播，要求事务的 Mapper 应声明 `tx = "mandatory"`。

`after_commit` 最多登记 256 项，按登记顺序执行，全部回调共享提交确认并释放事务借用后开始的 5 秒预算。调度前及任务实际调用回调前检查同一绝对截止点；预算耗尽或当前等待超时后停止启动后续回调，不按每项重新计时。调用方取消时请求取消当前回调并停止后续调度。回调失败、超期或未执行不撤销已提交数据，也不提供持久消息投递保证；已经进入的同步代码不具备硬实时中断保证。

同一事务一次只能借用一个连接槽，流也持有该槽；再次借用应先释放原句柄。在事务体返回时仍持有连接或流会阻止提交。无上游预算也受配置上限约束。`conn_for_budget` 在独立根连接接纳前固定请求绝对截止点与 `operation_timeout_ms` 上限的较早者，选池、原生租约监督和执行句柄共用它；尚未执行 SQL 或 SQL 已完成后仍持有连接，也受同一期限约束。环境事务内只缩短本次句柄的执行期限，保留事务原有总截止点与清理责任。

`read_with_budget` 约束完整读取的取消；取消单次读取不等于撤销仍在有效期内的整份租约。预算能终止合作式 VM、锁等待和结果消费，不能保证阻塞磁盘系统调用硬实时退出。过期租约的容量只能在真实原生关闭后回收；尚在执行或关闭的连接继续保活文件 owner。

`operation_timeout_ms` 限制单次 SQL，`transaction_timeout_ms` 从根事务入口固定整个环境事务的截止点。语句确认完成并释放借用后，业务可以在事务剩余时间内等待其它步骤；上一条 SQL 的短预算不会提前关闭事务，也不会因下一条 SQL 或嵌套调用而延长事务。期间仍受来源排空、隔离和未消费流的期限约束。

`stream_idle_timeout_ms` 限制等待首次消费和后续消费的空闲时间，覆盖小结果集及已有缓冲；等待数据库产生下一行的时间仍由操作总预算约束。生成的 Mapper 流在返回给业务前登记期限，执行单元在调用方不再轮询时仍监督到期。到期流交付一次错误后终结；事务内到期撤销提交权。过期句柄不能恢复执行，原生关闭确认后归还其逻辑借用容量，即使业务还保留旧连接或流。真实 worker 尚未结束时继续保留物理容量与文件 owner。

结果交付前显式复验操作总截止点、来源排空期限、隔离和最终关闭状态，已缓冲的行不能绕过。未终结流即使属于长事务，也受本次 SQL 的总期限约束；完成并消费结束的 SQL 才进入正常事务间隔。原生 progress/busy 回调同样读取消费到期事实，忙 VM 不依赖业务再次 poll 或 drop 才停止；调用方正在等待下一行时仍按数据库执行预算计时。

已确认的原始数据库错误保留类型与错误码，不因交付门禁或结果队列已满而替换为通用超时。流最多交付一次终止错误并释放内部责任。交付失败不代表数据库动作已经撤销：独立写入可能在消费超期前已提交，仍保留提交事实与缓存代际；调用方不能据此自动重放写入。

借用时先剔除失效空闲候选并选择其余健康连接，只有没有可用空闲连接时才在原有总预算内等待物理容量和建连。接纳新借用与空闲关闭决定互斥：接纳时把空闲监督切换到本次调用的绝对截止点；已经决定关闭的候选不可恢复，继续选择其它连接。失效连接的慢关闭仍占真实容量，但不会遮挡池内已有健康连接。

`acquire_timeout_ms` 只限制选池、等待容量及建连，不截短已接纳调用的操作窗口。取连接的等待仍消耗调用链原期限；借出后到首次 SQL 或首次流消费之前也受该期限约束。同一借用不因每条 SQL 重新取得完整预算，环境事务继续使用其独立的总截止点。

原生关闭可能删除 WAL、SHM 或 journal。检查若取得普通 sidecar 的零链接旧快照，会复查一次当前路径：已经不存在则按缺失处理，仍存在则继续严格检查类型、单链接及私有权限。主文件和 owner 锁不使用该规则，身份或链接异常仍立即拒绝。

低层 SQLx 的 Rust 类型解码发生在 executor 外部；若业务捕获写 RETURNING 的解码错误后还要继续事务，应调用 `connection.mark_write_failed()` 撤销提交权，或让事务体返回错误。Mapper 自动完成这一步。已经确认的 autocommit 写入不会因解码失败变成未提交。

## 维护、备份与恢复

`source.database_path()` 返回 owner 固定、父目录已解析的绝对数据库路径，供同时管理外部对象文件的宿主核对数据根；相对配置和父目录别名不影响该路径，不能据此绕过来源写活动文件。`retain_file_guard(Arc<T>)` 可以把宿主对象目录的独占权一并保活到原生连接全部退出且文件权威释放；每个 owner 只接纳一次，关闭开始后拒绝。守卫不得引用该 datasource 或间接形成所有权环。

`source.maintenance().inspect(&budget)` 执行固定的 `quick_check`、外键检查并读取 `user_version`。只有结构与引用检查通过才返回格式标识；异常或预算到期返回失败，不公开任意 PRAGMA。ALTER TABLE 添加 CHECK 约束时允许 SQLite 原生触发的内部检查，临时检查权在语句结束时撤销；独立业务 PRAGMA 及迁移中后续显式 PRAGMA 仍被拒绝。

`source.maintenance()` 提供固定维护操作，在同文件串行门内运行，拒绝环境事务。`checkpoint(mode, &budget)` 支持 Passive、Full、Restart、Truncate；默认在线选 Passive。结果分别报告 busy、WAL 帧数和已处理帧数，统计不可用时为 `None`。未完成 checkpoint 不表示已提交数据丢失，禁止删除活动 WAL。

`backup_to(new_path, &budget)` 使用原生 Backup API 每步最多复制 128 页，并验证 DONE、finish、quick_check 和外键。输出统一为 DELETE 模式的独立数据库。目标和 sidecar 必须不存在；目标锁、私有临时文件、禁止覆盖发布、文件与目录同步覆盖完整过程。只返回某个一致数据库状态，不承诺是调用开始瞬间的快照。取消或发布同步失败可能意味着结果未确认，必须检查实际目标，不能自动覆盖重试。

创建临时材料前利用已持有的目标 owner 锁，按文件系统实际名称查找排除临时主文件或 sidecar 与最终目标的等价名称；内部临时名称冲突时改选候选，不提前暴露最终目标，也不覆盖已有材料。候选耗尽时明确失败，不删除冲突材料。

原生维护失败保留 `MaintenanceError` 的 `operation`、`origin` 与 `code`，可从 anyhow 错误链读取。`Source` 表示源库故障，`Target` 表示目标准备、复制或校验失败，`Publication` 表示发布未完成或持久性未确认，证据不足时为 `Unknown`。checkpoint 的源故障与备份实际源读取故障进入相同的错误计数、缓存隔离和 napp readiness 链路：源库损坏立即隔离，连续三次源库 I/O 失败隔离。目标路径冲突、目标空间不足或发布失败不据此隔离健康源库，也不被当作源库恢复的成功证据。

已接纳备份的分页复制、目标校验与发布前检查共同遵守调用预算、来源排空截止点和隔离状态。文件同步返回后再次复验，超期且尚未发布时关闭目标并清理本次临时材料；原生 I/O 尚未返回时仍保留容量和文件 owner。发布一旦发生，继续确认目录同步并保留实际发布事实，不因随后超期删除目标，也不承诺底层磁盘系统调用的硬实时取消。

`restore_to_new(backup, new_path, journal_mode, &budget)` 独占已关闭的 DELETE 备份，再复制到不存在的新目标并明确转换模式。恢复共用备份的临时名称隔离、目标权威和完整发布约束。不能覆盖活动数据库；恢复后重新启动 Application 并验证迁移与业务状态。数据库备份不包含外部对象文件、密钥或业务目录。

## 观测与失败边界

Application 统一接入 datasource/driver 维度的连接等待、物理连接数及 Mapper SQL 指标。默认不输出原始 SQLx 语句和绑定值。Pool 数量统计真实原生责任；取消后仍在退出的连接继续占容量。业务错误保留 SQLx 类型与原生 code，提交未确认时隔离该 owner 的共享缓存。主文件或 owner 锁身份漂移、损坏、worker 异常退出及关键策略失效会持续隔离来源；连续三次 I/O 执行失败也进入隔离。`isolation_reason()` 保留首次原因，普通探针成功不能解除，需停止并重新创建来源。

停机先拒绝新根调用，已接纳事务只在自身与宿主排空截止点内继续；超过排空时间拒绝后续业务 SQL，回滚和物理关闭仍保留清理权。排空成功后在剩余预算内尝试 PASSIVE checkpoint，最后关闭物理连接；超时不会提前解除文件锁。

`classify_sqlite_error` 区分 BUSY、BUSY_SNAPSHOT、LOCKED、约束、只读、I/O、FULL、INTERRUPT 和损坏；分类本身不表示可安全重试。`engine_version_number()` 返回实际引擎标识。Application 还登记 `natx_sqlite_errors_total`、`natx_sqlite_maintenance_state` 和 `natx_sqlite_backups_total`，每源最多 23 个序列。状态包括隔离连接数量、来源隔离、WAL 字节数、可用空间、压力原因与最长读取句柄持有时间；指标抓取只读取内存状态，不发起文件 I/O 或维护操作。
