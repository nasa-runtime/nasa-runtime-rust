# 统一观测出口与平台适配

受管 Application 自动把已登记的接口、Mapper、连接池、通知和出口指标送入同一 MetricHub。
业务只维护应用 YAML；`napp` 托管指标出口，独立 `nafana-controller` 托管外部资源。观测网络失败
不改变 SQL 返回、事务裁决或数据库 readiness。

## 最小应用配置

```yaml
grafana:
  observability:
    enabled: true
    identity:
      environment: production
      cluster: orders-prod
      instance_id: ${POD_UID}
    provisioning:
      mode: platform
```

服务名默认取 Application 名。非本地环境必须提供 environment、cluster 和唯一 instance_id；
`local` 可由应用生成进程身份。service_name/environment/cluster 跨副本稳定，instance_id 在进程
生命周期内稳定且跨副本唯一。Kubernetes 使用 Pod UID，不使用可复用的 Deployment 或 Pod 名。
region、zone、service_version 可省略。与 telemetry 的服务名或实例别名冲突时在监听前拒绝启动。

platform 模式默认绑定 `0.0.0.0:9464/metrics`，其它模式默认 `127.0.0.1:9464/metrics`。
非回环端口必须由受信网络、NetworkPolicy、安全组、bearer 或 mTLS sidecar 限制。

## 抓取出口

```yaml
grafana:
  observability:
    enabled: true
    identity:
      environment: local
      cluster: workstation
    prometheus:
      scrape:
        listener: web
        path: /internal/metrics
        auth:
          mode: bearer
          token: secret://metrics_read
secrets:
  metrics_read:
    encoding: raw
    max_bytes: 512
    fragments:
      - env: METRICS_READ_TOKEN
```

`listener: web` 与 `server.health` 独立，路径位于同一 `server.context_path` 下。metrics bearer
只保护该路由，不借用业务 JWT。业务自动路由或手写 Router 占用该路径时拒绝绑定。
`listener: dedicated` 不需要 Web feature，适用于 worker 与 Batch。

并发超限返回 503；快照超时返回 503，但真实快照未结束前继续占用并发槽，避免客户端取消或连续
超时积累无界任务。认证失败返回 401，不申请快照资源。不记录 bearer 材料。

## Remote write

```yaml
grafana:
  observability:
    enabled: true
    identity:
      environment: production
      cluster: orders-prod
      instance_id: ${INSTANCE_ID}
    prometheus:
      export_mode: remote_write
      remote_write:
        endpoint: https://metrics.example.com/api/v1/write
        auth:
          mode: bearer
          token: secret://metrics_write
secrets:
  metrics_write:
    encoding: raw
    max_bytes: 1024
    fragments:
      - env: METRICS_WRITE_TOKEN
```

remote write 使用 protobuf、Snappy block 和有序 labels。单 worker 按时间戳顺序提交同一组累计
指标，队列满时丢弃最旧等待批次、保留最新累计状态。HTTP 失败只更新出口指标，不背压 SQL。
请求拒绝不无限重试；重试次数与退避同时受当前周期预算约束。停机先停止采样，再在配置预算内排空。
排空超时或任务取消销毁的在途、等待批次各计入一次 `dropped_shutdown`，与队列淘汰
`dropped_oldest`、普通失败 `failed` 和确认成功 `ok` 区分。计数仅保存在本地累计状态；进程退出前
若所有出口均不可用，这些终态不保证能够送到远端。
这不是持久化 WAL：进程崩溃或长期故障期间的瞬时 gauge 与中间采样可能丢失。

`max_batch_samples` 必须覆盖全部显式 source 预留、检查时已有的原生 cell 和当前接口目录估算；
Application 在所有组件 Ready 静态登记及 initializer 暂存任务工厂构造完成后，使用
`MetricHub::committed_series()` 计入原生 counter、gauge 及完整展开的 histogram，包括 initializer
新建的序列和 Web Ready 登记的静态源。组件与 initializer 终端主体在此期间均未经 poll；全部最终
门禁和共享启动预算复验通过、Application 发布 Ready 后才统一放行 remote write 和业务监听任务。
不足则报告 required/available 并拒绝启动，释放暂存任务与绑定端口，不静默裁剪 histogram。
独立装配 Exporter 时也必须在全部静态登记后调用 `validate_capacity()`；该方法只检查当前快照，不封闭 Hub。
这不承诺预测 Ready 后的新 label；动态增长仍受进程快照和 remote write 编码硬限约束。
remote write 没有 scrape 的 `up`，通过 `napp_observability_heartbeat_unixtime_seconds`
的最后样本年龄和外部平台期望实例指标判断失联；该信号不能区分进程停止、网络中断和 receiver 拒绝。常驻服务不使用
Pushgateway。`both` 仅用于受控出口迁移，平台必须选择唯一主数据源。

平台 remote write 规则通过
`provisioning.grafana.alert_rules.instance_down.expected_instances_metric` 引用外部平台的期望实例指标，
默认 `platform_expected_instance_info`。框架只生成查询，不维护实例清单、不提供库存客户端，也不生成该指标。
平台必须将其持续写入同一查询数据源，并提供 `deployment_environment`、`cluster`、`service_name`、
`service_instance_id` 四个身份标签；值为 1 表示本应在线，0 或不存在表示不纳入当前期望集合。
实例 ID 为 1–128 个 ASCII 字母、数字、`_`、`.`、`-`。附加平台标签或 HA 来源按业务实例主键取 max 去重。

例如，以下样本由外部平台提供，不是业务应用生成：

```text
platform_expected_instance_info{deployment_environment="production",cluster="hcm-prod",service_name="orders",service_instance_id="orders-0"} 1
```

配置引用可以改为平台自己的指标名，须匹配 `[A-Za-z_:][A-Za-z0-9_:]*` 且最多 128 字节；
不接受 PromQL 表达式、旧静态列表字段或已知应用自报库存/心跳名。平台负责随扩缩容和身份轮换更新期望。

规则读取有限窗口内的最后心跳，并为期望仍有效但窗口外缺失或从未上报的实例持续告警。
年龄异常与缺失路径保留相同实例主键，避免历史样本过期重置 `for`。
期望指标本身缺失或陈旧时，相关健康 baseline 保持 NoData，不宣称健康。
框架无法仅凭缺失区分计划退役与库存源中断；外部平台必须独立保障并监控期望源的持续性。
`napp_instance_info` 只说明实际采样，不是期望拓扑。

所有受管 Prometheus 样本附加 `napp_process_id`，由 exporter 初始化时生成并在其生命周期内保持固定。
它不是可配置业务身份，也不使用 scrape 的 `job`/`instance`。不同进程复用业务 instance_id 时仍形成
不同 remote write 时序，身份冲突规则因此保留两个来源。业务聚合继续使用既有身份主键，不按来源 ID 分组。

Mapper、Pool、通知和 exporter 根据静态目录精确预留系列；已有接口 Command 注册表允许运行期
增加命令，作为兼容 source 按实际样本受 MetricHub 统一硬限约束，不宣称具有静态精确上限。
remote write 对每个完整快照再次检查 max_batch_samples；运行期新增接口超过上限时拒绝该批，
记录出口失败，不截断 family，也不改变接口业务返回。

平台只接受能形成实际数据路径的组合：scrape-only 不接受 remote_write 发现；remote-write-only
只接受 auto 或 remote_write；both 由 discovery 决定主数据源。auto 先服从出口模式，只有可抓取时
才按 Kubernetes binding 或 Docker DNS 选择。发现资源、失联规则与 Grafana 健康基线使用同一裁决。
全部具体规则关闭时不创建空 rule group；关闭配置不删除已有平台资源，退役资源由平台显式管理。
非空规则组仍须逐条验证 owner，不能因确定名称而覆盖其它 owner 的规则。

## 递归默认与边界

所有可选配置树支持省略或 `{}`，同一叶子使用相同默认。未知字段、显式 `null`、非法 URL、非法
范围在网络动作前拒绝。禁用能力不读取其 secret、不创建端口、客户端或队列。

| 路径后缀 | 默认值 | 范围与条件 |
|---|---|---|
| `enabled` | false | 只控制出口与平台期望状态，不关闭基础指标 |
| `identity.service_name` | 应用名 | 1–63 个 ASCII 标识字符 |
| `identity.environment` / `cluster` | 无 | 启用时必填，分别最多 32 / 63 字符 |
| `identity.instance_id` | local 生成 | 非 local 必填，最多 128 字符 |
| `identity.region` / `zone` / `service_version` | 省略 | 分别最多 63 / 63 / 64 字符 |
| `prometheus.export_mode` | scrape | scrape / remote_write / both |
| `scrape.listener` / `path` | dedicated / /metrics | web 需 Web 能力；path 是长度不超过 128 的绝对静态路径 |
| `scrape.bind` | 按模式派生 | platform 为 0.0.0.0:9464，其它为 127.0.0.1:9464 |
| `scrape.max_concurrent_requests` | 4 | 1–32 |
| `scrape.request_timeout_ms` | 2000 | 100–10000 |
| `scrape.auth.mode` / `remote_write.auth.mode` | none | none / bearer；bearer 必须有 secret locator token |
| `remote_write.endpoint` | 无 | 启用时必填 HTTPS；local 允许回环 HTTP，不接受 URL user-info、query、fragment |
| `remote_write.interval_ms` | 10000 | 1000–300000 |
| `remote_write.request_timeout_ms` | 3000 | 100–30000，严格小于 interval |
| `remote_write.queue_capacity` | 8 | 1–64 个快照 |
| `remote_write.max_batch_samples` | 5000 | 100–50000，覆盖显式预留、已创建原生序列及当前接口目录 |
| `remote_write.retry_max_attempts` / `retry_backoff_ms` | 1 / 500 | 1–3 次、100–5000ms |
| `remote_write.shutdown_drain_timeout_ms` | 3000 | 0–30000，0 表示不等待排空 |
| `provisioning.mode` / `required` | disabled / false | required 只约束 controller 的部署观测，不改变业务 readiness |
| `provisioning.reconcile_interval_ms` | 30000 | 5000–300000 |
| `provisioning.request_timeout_ms` | 3000 | 100–30000，Grafana/Kubernetes/Prometheus 请求预算 |
| `provisioning.lease_duration_ms` | 120000 | 至少三个调和周期且覆盖两次请求预算，最多 3600000 |
| `provisioning.prometheus.discovery` | auto | kubernetes_pod / docker_dns / existing / remote_write / auto |
| `provisioning.prometheus.job_name` | 按部署身份派生 | 最多 128 个 ASCII 标识字符 |
| `provisioning.prometheus.scheme` | http | http / https |
| `provisioning.prometheus.scrape_interval_ms` / `scrape_timeout_ms` | 15000 / 10000 | interval 1000–300000；timeout 100–interval，且不小于应用快照超时 |
| `provisioning.grafana.endpoint` / `api_token` | platform binding | endpoint 必需；token 只接受 secret locator，由 controller 解析 |
| `provisioning.grafana.organization_id` / `folder` | 1 / nasa-runtime/服务名 | 正组织 ID；folder 最多 128 字符，显示标题附加 owner 标记 |
| `datasource.mode` / `uid` | managed / 派生 UID | existing 只校验并引用，不改写；managed 只更新自身 owner |
| `datasource.prometheus_url` | platform binding | managed 时必需；HA 必须指向去重查询层 |
| `dashboards.interfaces` / `mapper` / `datasource` / `notifications` | true | 独立开关，不删除既有资源 |

以上 scrape、remote_write 路径位于 `grafana.observability.prometheus`；datasource、dashboards 与
alert_rules 位于 `grafana.observability.provisioning.grafana`。

## 聚合告警默认值

这里的规则由监控后端按时间窗口求值，与 `sql.observability.alerts` 的单次 SQL 通知独立。
单次通知需要业务安装 `Notify`；逐条慢 SQL 通知还须开启 `alerts.slow_sql` 并设 `cooldown_ms=0`。
其耗时达到或超过 Mapper 的有效阈值即命中，不等于窗口 P99 超阈值。两条告警路径同时开启时应
选择各自明确的业务目的和路由，避免把同一事实当成两次数据库故障。

`alert_rules.enabled` 默认 true；`evaluation_interval_ms` 默认 60000，允许 10000–300000。
`notification_policy_ref` 可省略，设置后作为稳定路由标签交给平台既有 policy，不创建联系渠道。
所有规则的 enabled 默认 true，for_ms 允许 0–3600000；窗口至少覆盖两个 evaluation interval。
下表未出现的字段在对应规则中不合法。

| 规则 | 默认参数 | 默认 for_ms |
|---|---|---|
| error_rate | window_ms=300000，threshold_ratio=0.02 | 600000 |
| p99 | window_ms=300000，threshold_ms=1000 | 600000 |
| pool_saturation | threshold_ratio=0.90 | 300000 |
| acquire_timeout | window_ms=300000，threshold_count=1 | 0 |
| transaction_slot_wait | window_ms=300000，threshold_ms=250 | 300000 |
| stream_cancel_rate | window_ms=300000，threshold_ratio=0.05，minimum_calls=20 | 300000 |
| notification_queue | threshold_ratio=0.80 | 300000 |
| provider_failure_rate | window_ms=300000，threshold_ratio=0.10，minimum_deliveries=10 | 300000 |
| instance_down | expected_instances_metric=platform_expected_instance_info，引用外部平台指标 | 120000 |
| identity_collision | 无附加参数 | 60000 |

window_ms 为 60000–3600000，threshold_ratio 为 0–1，threshold_ms 为 1–300000，计数与最小样本量
为 1–1000000。实例失联保留实例维度；服务 SLO 聚合副本，避免单个节点分别触发同一服务告警。
单次 SQL 离散通知与窗口告警应明确唯一主路由；需要集群去重、静默和抑制时使用平台路由。

## 平台部署与权限

平台管理员安装 controller 后，以应用 YAML 路径启动 `nafana-controller application.yaml`；
`--once` 只执行一次收敛。Grafana 与 Prometheus 必须已存在，controller 不创建集群或安装第三方服务。
Grafana adapter 使用 Dashboard、datasource 和 alerting provisioning HTTP API；部署平台应提供
兼容这些 API 的 Grafana。受管面板不使用独立静态接口墙的实验性布局 schema。

### 同一份有效配置

controller 使用标准 `naml::YmlLoader` 合并主文件、显式 `APP_PROFILE` 对应的同目录
`<主文件名>-<profile>`、已启用配置中心的有序 imports，最后应用最高优先级 `APP__...` 覆盖。
例如主文件 `zcf/application.yml` 的 prod profile 为 `zcf/application-prod.yml`；不设置 profile 不加载它。
配置树引用、环境默认值、内嵌占位符和标量类型都遵守标准加载器语义。

`nacos.enabled=true` 时，独立二进制需显式编入真实配置读取能力：

```sh
cargo build -p nafana --bin nafana-controller --features controller-nacos
```

该路径复用 `config-boot`：先按 `nacos.imports`、后按 `yml.imports` 的声明顺序取得 Nacos 与 file 覆盖，
相对 file import 从主文件目录解析，再应用环境覆盖。不支持的传输、拉取失败、合并失败均明确退出，
不能依据未合并的基础文件将观测当作 disabled；optional import 仍遵守配置中心的可选源语义。
`nacos.enabled=false` 与应用一致，不连接配置中心或读取其 imports。

远端启动读取受 `application.startup_timeout_ms` 约束，默认 30000 ms。主文件、profile 及每份已取得
overlay 上限为 4 MiB；`application.*` 是本地启动合同，imports 改写时拒绝，不接受部分配置。
controller 在启动时冻结配置，不订阅运行期配置变更；观测配置改变后应协调应用与 controller 重启。

最终合并后才判断 `enabled/mode`，服务名、Web 抓取地址、平台 binding 和 Grafana secret 都来自同一棵树。
仅控制面消费的字段要求占位符全部解析；未挂载的业务独有引用不阻断 controller，业务实例的
`instance_id` 改用 `platform-controller`，不借用 Pod UID。secret 材料只解析选中的 Grafana 声明及其
config_path 引用，不读取数据库或通知的 secret fragment。配置中心开启时，controller 需单独获得其只读凭据。

### 平台 binding

平台 binding 可以位于同一 YAML 的 `provisioning.bindings`，或由平台注入等价环境变量：

服务名优先使用有效 YAML 的非空 `identity.service_name`，否则使用 `application.name`；只有两者
都未提供时，平台才通过 `NAFANA_SERVICE_NAME` 传入业务 Application 名。binding 不覆盖已配置的身份，
也不能遮盖应用名类型错误、空字符串或非法标识符；controller 不从文件名猜测服务身份。

| binding | 环境变量 | 条件 |
|---|---|---|
| grafana_endpoint | NAFANA_GRAFANA_ENDPOINT | YAML 未指定 endpoint 时必需 |
| grafana_token_secret | NAFANA_GRAFANA_TOKEN_SECRET | YAML 未指定 api_token 时必需，值仍为 secret:// 引用 |
| prometheus_url | NAFANA_PROMETHEUS_URL | managed datasource 缺 URL 时必需 |
| docker_dns | NAFANA_DOCKER_DNS | Compose 的逐副本 DNS，必须返回每个容器地址，不是 VIP |
| prometheus_config | NAFANA_PROMETHEUS_CONFIG | controller 独占的 Prometheus 配置文件，不覆盖已有人工文件 |
| prometheus_reload_url | NAFANA_PROMETHEUS_RELOAD_URL | Compose 必需，Prometheus 已开启受限生命周期 reload |
| owner_lock_file | NAFANA_OWNER_LOCK_FILE | Compose 所有 controller 副本共享同一个文件 inode |
| scrape_token_file | NAFANA_SCRAPE_TOKEN_FILE | Compose bearer 时为 Prometheus 可读的 token 文件路径 |
| scrape_token_secret | NAFANA_SCRAPE_TOKEN_SECRET | Kubernetes bearer 时为 PodMonitor 可引用的 Secret 名，键为 token |
| kubernetes_api | NAFANA_KUBERNETES_API | 默认从 KUBERNETES_SERVICE_HOST / PORT_HTTPS 派生 |
| kubernetes_token_file / kubernetes_ca_file | NAFANA_KUBERNETES_TOKEN_FILE / NAFANA_KUBERNETES_CA_FILE | 默认 ServiceAccount 挂载文件 |
| namespace | POD_NAMESPACE | 默认读取 ServiceAccount namespace 文件 |

Kubernetes 只给 controller 所属 ServiceAccount 授权本命名空间的 Lease 与 PodMonitor 读写。
业务 Pod 必须具有 `app.kubernetes.io/name=<service_name>` 标签，并命名 metrics 容器端口；
使用 Web listener 时，该命名端口指向 Web 服务端口；controller 自动读取同一 YAML 的
`server.port` 与 `server.context_path`，发现地址不会使用 dedicated 默认端口。
Prometheus Operator 必须选择相应 PodMonitor。扩缩容与 Pod 重建由 Operator 服务发现跟随，不抓
Service VIP 或 Ingress。每次控制面写入前重新读取 Lease，失权、过期或剩余预算不足时停止动作。
`lease_duration_ms` 写入整秒的 `leaseDurationSeconds` 时向上取整，创建与续约遵循同一规则；
协议权威窗口不会短于已校验的毫秒预算，最多延长不足一秒。接管与写前复验均按实际 Lease 秒值判断。

Compose 独立 Prometheus 配置使用 DNS SD；所有应用副本共用服务 DNS 名但返回独立地址。
controller 只接受已带自身 owner 标记的文件和 Grafana 资源，不删除未知资源。平台失败重试保持
确定 UID，不创建重复 rule group；未完成的本轮在下次调和继续收敛。

业务进程按用途解析 scrape、remote write 及业务显式引用的 secret，跳过仅用于 Grafana controller
的凭据。同一 ID 在业务配置中有活跃引用时保留材料；不应将控制面凭据与业务用途复用。
controller 只解析自身 Grafana secret，不读取数据库或业务通知凭据。不要把
Grafana token 环境变量或 Kubernetes 写权限挂载到业务 Pod。

## 指标与多集群含义

所有出口附加 service_name、deployment_environment、cluster、service_instance_id，以及可选
region、zone、service_version。job 与 instance 留给 Prometheus target，Mapper 热路径不构造
这些 label。`napp_instance_info` 表示实际采样库存，不是编排器的期望副本数。

QPS 按副本求和；失败率用失败总速率除以调用总速率；成功 P99 先过滤 status=success，再合并
histogram bucket，最后计算分位数。Pool 同时显示集群容量与最高负载实例，通知队列显示最拥塞
实例。所有速率面板使用 `$__rate_interval`，普通面板必须选单一 environment/cluster。

Mapper 页面分别展示逻辑方法的失败率、取消率、成功 P99 与数据库客户端耗时，缓存后处理失败
不会伪装成数据库失败。Stream 面板沿用方法筛选。通知成功以 dispatcher 的 `outcome=accepted`
为准，但该回执只证明业务适配器确认下游接受，不证明最终消息已送达或被读取。Provider 成功率按
全部副本的投递量加权，投递耗时按 provider 分别合并 Histogram。

零调用、无数据、target down 是不同状态，不用无条件零向量掩盖缺失。Prometheus HA 必须在查询层
先消除 replica 重复；同一进程不能经多地址重复采集。受管发现保留应用身份，并丢弃与授权服务、
环境、集群不符或缺失实例身份的样本；样本丢弃与实例身份重复由独立规则告警。业务面板和窗口
规则在聚合前共同排除重复身份与抓取失败的实例，库存和可用性面板仍展示这些异常。
Grafana managed alert 的健康零值基线使用同一实例门禁；不存在合格实例时保持 NoData，不能借基线
重新引入已排除的样本。实例失联与身份冲突规则保留异常实例，避免检测对象被业务门禁隐藏。

出口还提供 `napp_observability_scrapes_total{outcome}` 与
`napp_observability_remote_write_total{outcome}`。只包含封闭结果，不包含 URL、token、原始错误
或 SQL。基础 SQL 计时边界与裸 SQLx 限制由 Mapper 文档定义，Dashboard 不推断数据库服务端纯执行时间。
