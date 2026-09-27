# namapper-core

`namapper-core` 定义后端中立的 Mapper SQL 渲染、分页、排序、缓存和观测合同。数据库连接与 SQLx
类型实现在 `namapper` 和 `namapper-pgsql` 中。
静态方法目录和固定原子单元区分逻辑调用、真实数据库访问与流消费，缓存命中不会伪造数据库访问；
基础指标不受日志级别、慢阈值或通知开关影响。
达到配置阈值的 SQL 可逐条提交异步通知；业务主动安装通知实现，框架不选择消息渠道或下游协议。

## 观测架构与职责

宏生成静态方法身份，数据库 adapter 在真实 SQLx 调用边界记录事实，本 crate 冻结策略并分别维护
逻辑方法、数据库执行与 Stream 原子单元。日志、慢指标和通知共用同一阈值判定；指标出口在抓取时
生成样本，通知在独立受管 worker 内调用业务 `Notify`，两者都不参与事务裁决。

缓存命中不制造数据库执行，连接等待不计入 SQL 执行耗时；客户端计时也不等同数据库服务端纯执行
时间。直接使用裸 SQLx 不会生成 Mapper 方法身份或 Mapper 慢 SQL 通知。

## SQL 观测与配置

`Query`、`StreamQuery`、`Insert`、`Update`、`Delete` 和 `Execute` 统一采集调用量、时长、在途数、
结果分类和准确行数。逻辑方法结果与 SQLx 原始结果分别计数：数据库已经成功但缓存处理失败时，
不会把数据库操作误记为失败。`RowNotFound` 独立归类而不触发执行错误通知。

Stream 构造、首次 poll、首行、底层活跃读取和完整生命周期分别计量。消费者处理行的间隔仅进入
生命周期；从未 poll 的流只记录构造与取消，不增加真实数据库调用。取消和展开由守卫记一次终态，
析构路径不输出日志或通知。

同时启用 `nasa` 的 `application` 与 `mapper`/`mapper-pgsql` 时，`napp` 自动冻结策略、预留整个 SQL
系列预算并注册 source，不需要 Mapper Hook 或业务注册指标。独立宿主可使用
`observability::metrics_source()`，连接资源与 exporter 生命周期由宿主负责。

下列 YAML 显示全部 SQL 默认值；可省略任意父级或写空对象。凭据和通知目标没有虚构默认值，
`provider_ref` 可省略，默认使用业务通过 `nanotify_core::init` 安装的实现；没有实现时忽略通知。所有阈值、资源预算、路由与输出策略均可由 YAML 调整；
未知字段、显式 `null`、目录外覆盖和越界配置拒绝启动。

```yaml
sql:
  observability:
    console:
      enabled: false
      statement_level: debug
      include_parameters: false
      max_parameter_chars: 256
      max_parameters: 64
    metrics:
      record_rows: true
    slow_sql:
      threshold_ms: 1000
      log_enabled: true
      log_level: warn
      log_cooldown_ms: 0
      include_sql: false
      max_sql_chars: 2048
    execution_error:
      log_enabled: true
      log_level: error
      log_cooldown_ms: 0
      include_sql: false
      include_database_code: true
      max_sql_chars: 2048
    acquire_wait:
      threshold_ms: 250
      log_enabled: false
      log_level: warn
      log_cooldown_ms: 60000
    transaction_slot_wait:
      threshold_ms: 250
      log_enabled: false
      log_level: warn
      log_cooldown_ms: 60000
    alerts:
      slow_sql:
        enabled: false
        severity: warning
        cooldown_ms: 60000
        include_sql: false
        max_sql_chars: 1024
      execution_error:
        enabled: false
        severity: error
        cooldown_ms: 30000
        include_sql: false
        max_sql_chars: 1024
      acquire_timeout:
        enabled: false
        severity: error
        cooldown_ms: 60000
        purposes: [mapper]
    dispatcher:
      queue_capacity: 512
      max_in_flight: 4
      delivery_timeout_ms: 3000
      shutdown_drain_timeout_ms: 3000
      max_attempts: 1
      retry_initial_backoff_ms: 250
      retry_max_backoff_ms: 2000
    datasource_overrides: {}
    method_overrides: {}
```

| 配置 | 合法范围或语义 |
| --- | --- |
| `console.statement_level` | `debug`、`trace`；连接产生事件且日志 filter 同步放行 |
| `console.max_parameter_chars` / `max_parameters` | 16..4096 / 1..256 |
| `slow_sql.threshold_ms` | 1..300000；原始 duration 大于等于阈值命中 |
| 慢日志级别 / 错误日志级别 | `info/warn/error` / `warn/error` |
| 日志 `max_sql_chars` | 128..16384；默认不含 SQL |
| 等待 `threshold_ms` | 1..300000；连接等待阈值不得大于获取连接超时 |
| 等待日志级别 | `info`、`warn` |
| 所有冷却毫秒数 | 0..86400000；0 不限制频率，各进程独立 |
| 告警 `max_sql_chars` | 128..4096；不包含 bind 值 |
| `provider_ref` | 可省略；省略或 `default` 使用进程通知实现，未安装则忽略。显式命名引用为 1..64 ASCII 字符，以字母开头，后续允许字母、数字、`_`、`.`、`-` |
| `severity` | `info`、`warning`、`error`、`critical` |
| `purposes` | `mapper`、`migration`、`probe`、`direct` 非空且不重复 |
| `queue_capacity` / `max_in_flight` | 1..65536 / 1..32 |
| `delivery_timeout_ms` / `shutdown_drain_timeout_ms` | 100..30000 / 0..30000 |
| `max_attempts` | 1..5；不确定是否送达的请求不重发 |
| 重试初始 / 最大退避毫秒数 | 50..5000 / 初始值..10000 |

### 逐条慢 SQL 通知

要让每条达到慢阈值的 SQL 都提交通知，业务先 `nanotify_core::init(Arc<dyn Notify>)`，并设置
`sql.observability.alerts.slow_sql.enabled: true`、`cooldown_ms: 0`。阈值取
`sql.observability.slow_sql.threshold_ms`，原始 Duration 达到或超过即命中；关闭慢日志不关闭通知。
默认 60000 ms 通知冷却会抑制重复事件，不能用于逐条推送。通知由受管 dispatcher 异步调用业务实现；
队列满或投递失败保留稳定计数，不阻塞 SQL，不承诺网络故障下绝对送达。未安装实现仍直接忽略。
非零冷却按尝试入队占用，不以发送成功为条件；缺失实现、队列满或停止时也不撤销。
慢通知按方法独立冷却，错误通知还按固定错误分类分开冷却；冷却中的事件没有进入队列，不计作队列丢弃。

```yaml
sql:
  observability:
    slow_sql:
      threshold_ms: 1000
      log_enabled: false
    alerts:
      slow_sql:
        enabled: true
        cooldown_ms: 0
```

该例关闭慢日志但保留阈值通知；普通执行计 SQLx 活跃调用时间，Stream 计底层活跃读取时间。
同一次慢执行失败且已启用错误通知路由时，优先使用错误规则及其冷却，不额外发送慢 SQL 事件；
未启用错误路由时仍可命中慢通知。取消和展开不在析构时发送。

### 覆盖与冻结

覆盖按 `method > datasource > global` 逐叶继承，空覆盖不重置上层值。数据源可以覆盖 console、
慢日志、错误日志、连接等待、事务槽等待和告警；方法只覆盖慢日志、错误日志及两类 SQL 告警。
方法键为宏冻结的 `module_path::Trait::method`；不存在的方法和数据源不会被静默忽略。基础指标、
行数开关和 dispatcher 只允许全局配置。有效策略可通过
`Application::sql_observability_effective(datasource, method)` 获取，不包含 URL、SQL 或凭据。
这些策略在启动时冻结；修改需要重启，已有 Stream 始终使用原策略。

同时编译两个后端、无 Mapper 静态方法且默认池由 UserHook 提交时，启动先为两个后端候选预留
指标预算，首次提交的具类型 Pool 冻结真实后端。之后不能提交另一后端，Ready 前必须提交默认池；
实际连接获取超时仍在 Prepare 复验。

## 开发控制台与数据边界

`console.enabled: true` 让 SQLx 输出 prepared SQL。额外设置 `include_parameters: true` 后，Mapper
输出带方法和数据源身份的 `Parameters` 事件，按实际 bind 顺序显示允许的常见值与类型。
参数输出仅允许明确的 `local`、`development`、`dev` 或 `test` 环境。敏感参数名强制脱敏，未知类型
仅显示类型占位，不要求原 Mapper 参数增加 `Debug` 或 `Serialize`。

SQLx 普通与慢语句使用相同级别，内建慢阈值设为最大值，避免与 Mapper 慢日志重复。
未配置 console 时兼容已有 `log.level` 的 `sqlx::query` 指令；同时声明两种入口会拒绝启动。
热刷新全局日志级别时保留冻结的 SQL 指令。外部注入的现成 Pool 不能追溯更改已有连接的日志选项，
调用方应使用对应 driver 的 `build_pool_with_logging` 或显式配置 SQLx ConnectOptions。
未声明文件日志组件时，框架 fallback 控制台仍应用 SQL 开关；自定义全局 subscriber 不被替换，
由其宿主负责放行事件。

默认慢日志、失败日志和通知不含 SQL、bind、缓存 hash key、连接 URL 或数据库错误正文。
允许输出的数据库码仅保留有限 ASCII 字符。`include_sql` 是显式诊断权限：prepared SQL 中的业务
literal、注释或动态标识符仍可能敏感，应限制访问。SQL、错误原文、trace id 和参数均不能作为指标标签。
关闭参数输出时不会调用参数日志 subscriber。已命中的 Mapper 诊断会隔离日志过滤、格式化与
输出过程的 unwind，保持已经取得的 SQL 结果；Future 取消与 Stream Drop 仅记录原子终态。

## 指标与离散通知

Mapper 指标使用 `namapper_` 前缀，连接指标使用 `natx_` 前缀。Histogram 以秒导出，固定桶保证跨实例
可聚合，`count`、bucket 和 sum 由同一原子采集层导出；不在 SQL 热路径写 MetricHub 或调用自定义 observer。
Prometheus 与 OTLP 读取同一组结构化 source。

配置 `alerts` 后，规则命中只做原子冷却和有界 `try_send`。后台 dispatcher 负责 provider 调用、超时、
有限安全重试及停机排空；队列满、通知失败、panic、超时或外部平台不可达都不能改变 SQL 返回、事务
裁决或数据库 readiness。同一次慢执行失败且已启用错误通知路由时，优先发送执行错误事件，
并保留 `slow` 事实和两类指标；错误规则冷却中不会再回退慢通知。
通知是非持久的尽力投递，不提供跨副本去重；窗口告警由监控后端聚合。
投递超时从 worker 开始处理时计算，包含安全重试与退避，但不包含排队和等待启动放行；队列没有 TTL。

渠道合同与业务注册配置见 [nanotify-core](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nanotify-core/README.md)。
业务主动实现 `Notify` 调用通知微服务，框架不选择通信协议或直接连接消息渠道。
抓取、remote write 和平台资源见 [nafana](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nafana/README.md)。

## SQL 与缓存合同

结构化 SQL 使用 `SqlNode::{Text, Bind, BindList}` 表达文本与参数边界。`render_postgres` 只在 bind
节点产生连续 `$n`，不会扫描或改写文本中的 `?`，因此字符串、quoted identifier、块注释和 JSON
操作符不会与参数编号混淆。空列表没有统一 SQL 业务语义，会明确返回错误。

分页通过有界 `PageRequest` 提供可直接 bind 的 `limit`/`offset`；动态排序通过 `MapperOrderField`
白名单输出列名；`MapperCacheMeta`、`MapperL2Cache`、codec、metrics 和 after-commit 清理目标用于让不同
数据库 adapter 共享同一缓存语义。进程内 single-flight、版本化/fallback codec 和默认注册槽也在本
crate 单点实现，因此同一个手写缓存、codec 或指标实现可同时供 MySQL 与 PostgreSQL Mapper 使用。

启用 `redis-cache` 后还提供 `RedisMapperL2Cache` 与
`RedisDistributedSingleFlightMapperL2Cache`。Redis 实现只依赖缓存合同，不依赖 SQLx driver；
PostgreSQL-only 应用使用它不会引入 MySQL runtime。

## 主要边界

该 crate 不创建数据库连接、不执行 SQL，也不根据数据库 URL 选择后端。普通应用应使用 `namapper` 或
`namapper-pgsql`；adapter 与宏实现可直接依赖本 crate 的稳定合同。Redis 连接仍由业务或受管应用创建，
独立模式由调用方显式安装默认 Mapper L2，Application 模式由命名配置完成安装与关闭。

## Application 接入

门面开启 `application,mapper-redis-cache` 或 `application,mapper-redis-cache-pgsql`，并声明
`"redis"`。`mapper_cache.enabled` 与 `redis_ref` 选择受管 Redis，准备阶段验证 Hash 字段过期能力；
Service 与 Batch 均在查询前完成门禁。

默认 L2 由 `MapperCacheOwner` 管理，旧句柄关闭后拒绝读写，旧 owner 不能撤销后续安装。
codec/metrics 通过 `configure_mapper_defaults` 纳入启动回滚与停机，无需另建关闭流程。
`GroupedCache` 和独立 `RedisMapperL2Cache` 保留各自 TTL 合同；后端不支持字段过期时不能宣称
字段级过期已经生效。

配置与完整生命周期边界见 [受管能力合同](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/managed-capabilities.md)。
