# 变更记录

所有面向使用者可见的变更都记录在这里。

本仓库是多 crate 工作区。只影响单个 crate 的变更，需要在条目里写明 crate 名；影响宏展开代码、`nasa` 门面、feature 开关或配置键的变更，需要写清楚可见行为和迁移方式。

## 未发布

- `nasa`：新增稳定 `outbox`、`idempotency`、`idempotency-mysql` 与
  `idempotency-redis` 门面 feature；业务无需再直接依赖实现 crate。
- `nadis`：`stream.async_del_record_period_ms` 现在真实驱动 ACK 后有界异步 XDEL；0 表示禁用，
  删除失败按周期重试，停机在业务排空后做末次 flush；`RunningPartition::async_delete_pending`
  暴露待删除量，便于对持续失败导致的消费反压告警。
- `nadis`：Stream builder 现在支持单点和 Redis Cluster。Cluster 订阅使用独立连接并按单个
  stream key 的 slot 路由，支持 MOVED/ASK；API 仍不提供跨 slot 的多 stream 阻塞读取。
- `nadis`：Stream 的阻塞读、非阻塞 idle、错误退避、重连、handler 和 XACK 全部响应订阅取消；
  启动拒绝空/超长名称、非法 After 游标、无效时长、零值或超过 10000 的批量，直接 XREAD API
  使用同一批量上限。
- `nadis`：`Leader`、`RunningPartition` 和 `RunningProxy` 增加 Drop 最后防线，避免调用方遗漏显式
  shutdown 后后台 owner/consumer 继续运行；Partition 并发 shutdown 现在按组单飞并等待同一收口。
- `nadis`：Redis 分区配置增加隔离组数量、topic 数量和所有组 resolved 分区总数上限；PROXY 增加
  consumer/task、批量和时长边界，默认启用近似 stream 长度上限。
- `nadis`：命令、Stream、分区组和停机 timer 统一限制在 Tokio 可安全表示的一年范围内；
  `Duration::MAX` 不再在锁、Pub/Sub、分区管理命令或 Redis TTL 转换中引发 deadline panic、
  `u128 -> u64` 回绕或 `u64 -> i64` 负值，超界输入会在 Redis 副作用前返回配置错误。
- `naws`、`nafana`、`hystrix`、`naauthz` 与应用并发/有界导出队列的非 fallible 入口统一
  收敛极端时长和容量；客户端心跳、集群 TTL/墙钟查询与优雅停机不再因越界输入溢出。
- `namapper`：Redis L2 在 HSET 前校验 TTL，避免过期参数失败后遗留永久字段；分布式
  single-flight 的锁、等待和轮询时长统一限制在 1ms–365 天。
- `ncrypto` / `naweb` / `nasa`：legacy RSA 私钥路径增加编译期 feature 与 Web 运行时开关双门；
  默认 `crypto`、`web-security` 和 `full` 不开放该路径，迁移调用方需显式启用
  `crypto-legacy-rsa` 或 `web-crypto-legacy-rsa`。
- `naimg`：默认格式集合收敛到 README 声明的常用格式，移除未承诺的 AVIF 编码链和对应停止维护
  传递依赖。
- 文档：澄清 Redis Cluster 分区组同槽边界，以及 OpenAPI、Schema Registry、业务 proto 的
  breaking gate 由真实业务合同 owner 仓库负责。

## 1.0.0 - 待发布

- 全部工作区组件统一采用初始版本 `1.0.0`，稳定入口遵循 SemVer；显式 experimental feature
  在晋升前不进入 `full`，也不纳入稳定 API 兼容承诺。
- Web 公开能力完成一次发布前命名收敛：运行时包 `mapping` 改为 `naweb`，过程宏包
  `mapping-macro` 改为 `naweb-macro`；`nasa` 门面模块由 `nasa::mapping` 改为 `nasa::web`，
  feature 由 `mapping` / `mapping-auth` / `mapping-crypto` / `mapping-security` 改为
  `web` / `web-auth` / `web-crypto` / `web-security`。`#[get_mapping]` 等属性、
  `MappingPlan`、`MappingRuntime` 与 `configure_mapping` 保留原名，因为它们表达的是 Web
  子系统内的具体声明式路由机制，而不是组件包名。此项是源码级 breaking change，不提供旧路径别名。
- `nafka`:解码器 panic 不再穿透成 group `Crashed`,改按 `invalid_record_policy` 处置为"确定无效记录";
  panic 原文**不再外传**——它会进公开的 `GroupHealth.last_error` 与 DLT reason header,而
  `from_slice::<T>(p).unwrap()` 这类写法的 panic 文本里嵌着出错的输入片段(凭据外泄面)。
  现只保留固定分类文本与消息类型名。
- `nafka`:新增 `behavior.decode_failure_halt_streak`(默认 `Some(1000)`,`None` 关闭)。
  连续解码失败达阈值即 Halt 分区,任一次解码成功清零。没有它时,"新版本解码器对所有记录都失败"
  会让整个 topic 满速排进 DLT、offset 一路前进而 group 仍报 Running。
- `nafka`:`producer_lane_queue_utilization` 修正双计——`fire()` 同时占 observer permit 与 `active`,
  原式把同一条在途消息数两次,半载即报 100%。现分子分母都只取 fire observer 容量,`active` 只发绝对值;
  两个 gauge 去掉 `topic` 标签(lane 级量带 topic 会让 `sum by (lane)` 翻 N 倍)。
- `nafka-macro`:`event`/`group`/`client`/topic 元素拒绝首尾空白(源码级 breaking):
  这类值以前编译通过但在运行期静默不匹配或生成另一个 group.id。
- `naws`(feature `kafka`):启动期的帧预算交叉校验由拒绝启动改为告警。原判据重复计入了路由 header
  预算,与 topic 契约的下界形成空区间,使**全默认配置无法启动**。告警会点名 `behavior.max_record_bytes`
  与 `ServerConfig.max_frame` 两个旋钮。

- `hystrix`:修复零值语义与文档相反的缺陷——`max_concurrent = 0`、`timeout_ms = 0`(或零时长
  `Duration`)在 yml、注解和显式 `Command` 三条路径上都曾被原样传下去,建出 0 许可信号量(每个请求
  429)和 0 毫秒超时(任何带 await 的 handler 立刻 504)。现统一在构造时归一化为"不限并发/不超时"。
- `hystrix`:新增 `rollingCountCanceled`。执行 future 在产生结局前被丢弃(客户端断连、外层包装取消、
  handler panic)时补记一次取消,并计入 `requestCount` 与 `errorPercentage`;取消不产生延迟样本、
  不触发降级。官方 Hystrix Dashboard 会忽略这个未知字段,错误率仍如实体现。
- `hystrix`:`IsolationRule` 拒绝未知字段(`timeoutMs` 这类拼写错误不再被静默忽略);同 (group, name)
  重复构造 Command 时输出 `warn`;注册表与滚动窗口不再因一次持锁 panic 而永久不可用;
  `current_tps()` 改走只累加计数的快路径,不再为 TPS 复制并排序延迟样本。
- `hystrix-macro` / `nafana-macro`:handler 原返回类型改为保留在返回位置(内层 `async fn`),
  修复 `-> Result<T, E>` 与 `-> Result<impl IntoResponse, E>` 因类型信息丢失而无法编译的问题
  (E0282/E0283);`impl IntoResponse`、无返回值和带 extractor 的写法行为不变。
- `naweb-macro`:`#[*_mapping]` 写在 `#[hystrix]` 或 `#[grafana]` 上方时统一编译报错。此前只拦
  `#[grafana]`,`#[hystrix]` 顺序写反会静默退化成函数名并丢失真实路由。
- `nafana`:Dashboard 卡片新增 `Canceled` 计数格,避免"错误率不为零但成功/失败/超时/拒绝都是 0"
  无法归因;README 补充取消结局、双层保护结局不一致、未知配置字段后果和对应常见问题条目。

- 新增 `nafana` + `nafana-macro`:独立的接口级隔离监控组件(合同 `nafana/nafana.md`)。
  提供 bulkhead/超时/降级/配置驱动 isolation(yml 根 `grafana.isolation`)/CostTime 聚合日志；
  观测出口为 Prometheus 文本 `GET /metrics`(`nafana_*` 前缀,单调 counter +
  10s 精确分位 gauges + 直方图双轨),官方 Grafana 面板随 crate 交付
  (`nafana/dashboards/nafana-interfaces.json`,含 Total QPS/TPS 顶栏)。面板使用 Grafana 12.1
  原生 V2 Auto grid(220px 最小卡片宽度、最多 10 列、窄屏自动换行),不依赖第三方插件；同目录
  提供 `prometheus.yml`、Grafana feature 环境变量和 API 导入说明，可复制后只改应用/目标字段。
  门面 feature `grafana`,入口 `nasa::grafana` + `#[grafana]`;重复贴 `#[grafana]` 会编译报错。
  `max_concurrent=0`/`timeout_ms=0` 统一归一为不限制/不超时；
  并发峰值同时导出 10s 滚动口径(会回落)与终身口径；超时后的 fallback 不再占用 bulkhead
  许可或 `nafana_inflight`；配置驱动隔离仅在完整路径段边界剥离 context-path，重复初始化会明确告警。

- 补齐所有工作区组件的 README 覆盖，保证每个组件都有用途、配置、初始化和示例说明。
- 补齐贡献、安全、测试、发布和公开发布检查文档。
- 明确双许可证发布方式：MIT 或 Apache-2.0。
- 对象存储与 gRPC listener 以显式 experimental feature 交付，不进入 `full`；Kafka Schema
  Registry 同样保持 experimental。
- 强化 mapper、事务、缓存、Redis、WebSocket、服务发现、日志、调度和基础工具等组件的业务注释与 README 使用说明。
- 将项目发布名统一为 `nasa-runtime-rust`，并更新下游 Rust 项目的本地 path 依赖。

## 版本条目模板

```markdown
## 1.x.y - YYYY-MM-DD

### 新增

- `crate-name`：说明新增的公开能力和适用场景。

### 变更

- `crate-name`：说明行为、API、feature 或配置键变化。

### 修复

- `crate-name`：说明修复的问题、触发条件和影响范围。

### 兼容性

- 说明需要开启的 feature、变更的 yml 键、迁移步骤或行为差异。
```
