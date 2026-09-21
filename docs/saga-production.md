# Saga 生产运行指南

本指南描述当前受管 Saga 的产品合同。核心价值是把跨服务业务意图收敛为本地 ACID、Outbox 至少一次、
Inbox 幂等、持久状态机和显式补偿组成的可恢复事实链。它不提供跨服务 ACID、物理 exactly-once，
也不替业务解决多个并发 Saga 对同一资源的竞争。

协调侧 client、result 与 Registry 支持固定地址或受信 Nacos 服务发现，逐实例绑定身份、端点和有效路径。
HTTP HMAC 与 gRPC mTLS 随配置视图原子轮换，并保留有界的旧凭据重叠窗口。definition 提供
candidate→active→deprecated→retired 生命周期，退休前在数据库事务中检查保留引用。start 支持
原始 bytes 与显式 schema，HTTP/gRPC 共用业务校验、幂等摘要、权限、分页和管理事务。

## 角色与所有权

saga.role 是部署取得权限的唯一开关，不根据已链接代码、Web 组件或数据源数量推断。

| 角色 | 持久资源 | 入站与运行能力 |
| --- | --- | --- |
| orchestrator | instance、journal、result Inbox、command Outbox、timer、Catalog、审计、配额 | start/query/audit/管理、result、Definition Registry、timer |
| participant | command Inbox、gate、业务事实、result Outbox | command、业务 handler、capability 续租 |
| client direct | 无 Saga 表 | 远程 start/query |
| client reliable | start-intent Outbox | 事务内可靠发起、远程 query |
| combined | 两个数据角色并集 | 必须显式设置 allow_combined_role，并使用同一事务 datasource |

managed 是默认 plan_mode。业务调用 configure_saga 只允许用于明确的 custom 模式；两者同时存在会
拒绝启动。participant 和 client 不会先创建完整 Orchestrator 再关闭 timer，它们从构造、建表、路由
和监督阶段都不取得全局推进权限。

## 零装配 Orchestrator

HTTP 受管路径的业务入口可以只保留 Application 声明；gRPC、Kafka 或 Redis Streams 数据面只需把
对应组件加入声明并切换配置，不需要增加 Saga 装配代码：

~~~rust
#[nasa::application("saga", "web")]
async fn main(_app: nasa::Application) -> anyhow::Result<()> {
    Ok(())
}
~~~

示例配置：

~~~yaml
datasources:
  saga-control:
    driver: mysql
    url: ${SAGA_DATABASE_URL}

outbox:
  datasource_ref: saga-control

saga:
  role: orchestrator
  plan_mode: managed
  service_identity: checkout-orchestrator
  replica_identity: ${SAGA_REPLICA_ID}
  orchestrator:
    datasource_ref: saga-control
    timer_poll_interval_ms: 500
    timer_error_backoff_ms: 1000
    timer_operation_timeout_ms: 5000
  definition_catalog:
    mode: dynamic
    datasource_ref: saga-control
    activation_policy: validated
    watch_interval_ms: 500
    capability_registry_ref: saga-participant-capabilities
    publisher_authorization_policy_ref: saga-definition-publishers
    publish_tenants: [system]
    signing_keys:
      checkout-owner-key: checkout-owner-public-key
  http:
    base_path: /_nasa/saga
  api:
    page_token_key_ref: saga-api-page-token
    http:
      enabled: true
      expose_admin: true
      expose_definition_registry: true
      authorization_policy_ref: checkout-saga-http-rbac
      callers:
        order-api:
          credential_ref: saga-order-client
          tenants: [system]
          permissions: [start, read]
        checkout-workflow-owner:
          credential_ref: saga-definition-publisher
          tenants: [system]
          workflows: [order_checkout]
          permissions: [registry]
  transport:
    address_policies:
      saga-participant-routing:
        http_schemes: [https]
        http_hosts: [inventory.internal, payment.internal]
        http_ports: [443]
    command_result:
      kind: http
      http:
        shared_replay_claim: saga-http-replay
        command_credential_ref: saga-command
        result_credential_ref: saga-result
        request_timeout_ms: 5000
        body_limit_bytes: 1048576
        concurrency_limit: 256
        routing:
          mode: capability-registry
          address_policy_ref: saga-participant-routing
~~~

`address_policy_ref` 必须命中 `transport.address_policies` 中的实体政策。HTTP 逐个校验
scheme、规范 host 和可选 port allowlist；`http_ports` 为空时仅放开已列 host 的任意端口。
gRPC 始终要求 HTTPS/mTLS，Kafka 和 Redis Streams 按分隔符边界校验 topic/stream 前缀。
Catalog 中的 capability 未通过该政策时不能激活 definition，也不能进入路由快照。

secret material 只通过 Application secret reference 或环境注入。`page_token_key_ref` 指向不少于
32 字节且在全部 API 副本间一致的密钥，使分页令牌可跨重启与负载均衡实例校验。
service_identity 是逻辑服务身份，
同一服务全部副本相同；replica_identity 必须逐副本唯一且重启稳定，用于 timer 租约和审计。运行实例
还生成不可配置的 fencing nonce，失权副本不能仅凭复制配置继续推进。

## 流程定义与 capability

业务仍拥有完整流程语义。workflow owner 用 `#[nasa::saga_workflow]` 返回 `WorkflowDefinition`，明确步骤
顺序、owner、补偿能力、pivot、取消、resolve 与 timeout。participant 用 `managed=true` 的 `#[saga]`
实现真实业务副作用。业务 main 不调用 publish/register，Orchestrator YML 不枚举 workflow、step 或
participant owner。

workflow owner 所在 participant 或 client 在动态模式下配置 `definition_catalog.registry_client`，声明
HTTP/gRPC Registry 地址、在线认证凭据、`definition_signing_key_id` 与私钥 secret reference。`napp`
收集全部本地 definition，为每个 `publish_tenants` 形成 canonical artifact 和 Ed25519 detached signature，
并持续发布到取得匹配持久收据为止。Orchestrator 的 `definition_catalog.signing_keys` 只保存 key id 到
公钥 secret 的引用；服务端同时复验认证 owner、tenant 权限、canonical bytes、SHA-256、签名和 definition
seal。私钥不能进入 Orchestrator 配置，在线 HMAC/mTLS 凭据也不能代替 definition 签名密钥。
Registry caller 必须同时声明非空 `workflows` 授权范围；每项是精确 workflow 或 `*`。服务端在读取、
发布、激活、废弃 definition 以及登记 capability 前同时复验 tenant 与 workflow，只有 `registry`
permission 而没有匹配 workflow 不能进入控制面存储。

动态 Catalog 持久化完整 DefinitionArtifact。相同 tenant、workflow、definition_version 与 digest 的
发布仅在 owner、canonical artifact、digest 与 seal 全部相同时幂等，任一事实不同都冲突；active 内容
不能原地覆盖。deprecated 只阻止新 start，已运行实例继续按创建时冻结的 version/digest 处理 timer、
result、resolve 和补偿。
已提交 Start 在 deprecated 后仍按原请求摘要返回 Duplicate 或 Conflict，运行中与终态实例一致；
认证、租户权限、定义摘要和事务执行资格仍须有效。只有真正的新建适用 active 门禁，拒绝时同事务
回滚全部创建事实。因此，首次回执丢失后的可靠 start-intent 可通过 Duplicate 结清，不重复发布首步命令。

participant 在 listener 可用后自动发布并续租 CapabilityDescriptor。descriptor 把认证 owner、replica、
workflow/version/step、transport、origin、effective_saga_base_path 与 route generation 绑定。Catalog
在 capability 主键行锁内分配单调 route generation 并通过 HTTP/gRPC 收据回传；同内容重报复用已提交
代际，端点变化原子取得下一代，墙上时钟只参与消息时效与租约观察。租约
到期只移除当前路由，不删除持久 definition。HTTP/gRPC capability 入口只要求组件已启动且未停止，
参与方租约自然到期或 Orchestrator 重启后仍可在同一 listener 续租；身份、租户、workflow、地址政策和
协议认证中的 replay 检查始终生效。缺少 route 的 command 保留在 Outbox，Catalog 监督循环在 route
完整前保持业务摘流，单次续租成功不等于新 start/claim 已获准。

依赖 Catalog 资格的 HTTP 业务请求在异步认证之后复验资格；HTTP/gRPC start 在读取 definition 快照前冻结本次
执行期限和撤销标识。连接池或行锁等待之后，创建事务每次恢复执行及交给事务层提交前都复验资格，
失权则由事务层回滚实例、配额和 Outbox，续租或新快照不能延长旧操作期限或恢复已撤销的操作。
COMMIT 发出后仍以数据库提交收据或提交结果不明为准，即使响应跨过期限，也不能宣称确定回滚；
调用方应使用同一幂等请求核对最终事实。

validated 策略复验 definition、capability、route、安全合同和有效副本 generation/digest 确认后激活；
任一副本落后时 Catalog readiness 摘流并停止接受新 start/claim。approved 策略保持 candidate，直到具备
Registry 管理权限的主体显式批准。普通 capability 租约、start client 和 Saga 管理权限不会隐式取得
definition 发布或激活权限。

Registry 的参数、权限、不存在、业务前置条件与摘要/操作身份冲突使用确定性 HTTP 4xx 或 gRPC code；
数据库断连、超时和事务结果不明统一返回 HTTP 503 或 gRPC `UNAVAILABLE`。发布器和 capability 续租器
只能对后一类失败保留原请求重试，不能把暂时性存储故障记成不可重试的 definition 冲突。

## HTTP 路由与安全

全部 Saga HTTP 路由复用 Application Web listener。saga.http.base_path 是 context 内唯一子路径，默认
/_nasa/saga；实际基础路径严格等于 server.context_path 与 base_path 各拼接一次。base_path 不能是根、
不能包含尾斜杠、重复斜杠、点段、percent encoding、query、fragment、模板或通配符。
当 `server.health` 启用时，Saga 前缀不得遮蔽框架 `/healthz`、`/readyz`。统一观测配置的 Web 指标
路径独立于 health 开关保留；未编入统一观测能力时，health 才联动保留 `/metrics`。
冲突在绑定 Web listener 与发布 Ready 前拒绝。未启用的框架入口不参与路径保留。

标准路由包括 instances、results、query、audit、pause、resume、retry-compensation、retry-resolution、
manual-close、registry/capabilities、registry/definitions 与 metrics。具体写入口按角色、transport 和
expose 策略裁剪。Saga 保留前缀由框架专用分支处理，不进入业务 Router 的 middleware、fallback 或 nest；
业务注册冲突路径会在路由封口时拒绝。启用 Saga HTTP 时不接受不透明的 `configure_router`；手写业务
使用 `configure_router_scoped` 声明非根静态前缀，内部使用相对该前缀的路径，同前缀按登记顺序组合。
内部路径统一挂在声明前缀之下；业务前缀及自动路由模式与 Saga 相交时拒绝 Ready。最终 listener 先按完整
Saga 前缀选择专用 Router，再进入业务 Router，因此未知路径、method、静态路径优先级都不能跨越认证域。

HTTP activate/deprecate 请求体必须携带 `expected_sha256`、`operation_id` 与 `reason`；同一 actor、动作和
operation_id 只允许重放完全相同的持久裁决。管理写请求携带 `operation_id`、`reason`，并可携带
`expected_state_version`、`expected_control_version`；expected version 在持锁事务内复验，不匹配返回
冲突且不产生状态、审计或 Outbox 副作用。gRPC 与 HTTP 复用相同裁决语义。

HTTP audit 接受签名 body 中的 `page_size` 与 `page_token`，gRPC `GetAuditTrail` 使用同名字段。响应按
数据库分配的全局 `audit_seq` 输出统一 records，不按类别或内存数组 offset 定位。attempt 开始与终态变化
分别形成不可变事件，其它 transition、control、management、conflict 事实也与业务事务一起追加事件。
`next_page_token` 由 `page_token_key_ref` 对租户、实例和末项序号共同认证；每条记录都返回数据库生成的
发生时间，任何非空页都会返回末项 checkpoint。trigger 在分配序号前递增按 Saga 隔离的提交 guard，
因此同一实例的并发事务不会出现高序号先提交；历史超过 1000 条或读取后继续追加时，续页仍只沿全局
序号前进。

HTTP 签名覆盖认证 producer、实际规范 path、timestamp、nonce 和原始 body。sender 使用 capability
发布的逐实例 origin 与 effective path，不从 service name 猜 context path，不跟随 3xx。共享 nonce
claim、tenant 权限、body/并发上限和收据裁决由受管分支统一执行。多副本不能把进程内 replay cache
作为唯一权威。

只有 Committed 或 Duplicate 允许源 Outbox 前移。连接失败、deadline、回包丢失、429、5xx 与响应体
不确定都保留相同 event_id 重投；确定性身份或合同拒绝只有在 durable DLT 政策允许时才能越过。

gRPC command/result 的 `request_timeout_ms` 覆盖连接就绪、响应头、正文和最终 trailers 的整个传输
等待；各阶段共享截止时刻，调用者更短的期限不会被延长。正文或 trailers 到期未完整返回时，客户端
报告 DeadlineExceeded，原事件仍按不确定投递处理。动态 command 副本在一轮共同预算内逐成员检查
health，仅明确 Serving 的集合复用已探测连接轮询承担新投递；单个失败成员不否定其它成员，全部不服务时拒绝发布路由。
凭据或发现刷新包含在该原期限内。取得首个 Serving 响应后，其余成员探测最多使用当时剩余预算的
一半，为业务 RPC 与完整收据留出时间；到达收集边界仍未完成的成员不进入本次投递集合，下次继续复验。
独立的 Ready/Catalog health 门禁仍使用自己的完整探测预算，调用者的短期限不会被重新计时或延长。
共享 channel 的重叠调用在逐请求核对发现与凭据后，可复用等锁期间完成的同代健康结果，
避免重复探测消耗各自的收据预算；后续独立调用仍重新检查 health。探测结束后再次查询发现并
核对凭据，成员或信任窗口变化时重新探测，不发布旧权威下的结果。连接集合的发布和失败撤下
在同一锁内完成，等锁超时不会撤销其它请求的路由；持锁请求取消后，等待者可接续探测，
不会留下后台刷新任务。所有等待、复验与收据读取继续受各请求原截止时刻约束。

## 本地事务与即时投递

Orchestrator start 在一个 datasource 事务内提交 start 幂等事实、instance、首条 command Outbox 和
timer。result 处理在一个事务内完成 Inbox claim、attempt/journal、实例 CAS、下一 command Outbox 与
timer。participant 在自己的事务域内提交 command Inbox、gate、业务事实与 result Outbox。

数据库明确确认 COMMIT 后，只发布按 driver、datasource_ref 与 lane 限定的有界可合并唤醒。after-commit
不执行 HTTP、Kafka、Redis Streams 或 gRPC 网络调用。统一 dispatcher 立即领取数据库中最早的有序
前缀，并继续服从 claim、批处理、退避、DLT 与停机门禁；新提交不能越过失败旧事件。

poll_interval_ms 只承担漏信号、跨进程追加、进程退出和崩溃恢复。信号丢失不影响正确性，数据库
Outbox 行始终是唯一持久事实。多副本本进程唤醒不替代数据库 claim 或 Inbox 幂等。

## 发起等级

direct client 不创建本地数据库或 Outbox。调用方生成稳定 saga_id、trigger_id、business_key 与
definition_version，使用 SagaRemoteClient::start/get；结果不明时保持完全相同的请求重试。

reliable_start 要求 client.datasource_ref。业务在当前同源事务内先写业务事实，再调用 enqueue_start；
框架追加 SagaStartIntent，二者同时提交或回滚。远端不可用时请求可以返回本地已受理，但不能宣称 Saga
已创建；事件恢复投递后只有 Committed 或 Duplicate 才标记完成。超过业务发起期限的积压需要告警或
人工处置。

可靠 client 的 Outbox dispatcher 与业务事实、start-intent 共同绑定 `saga.client.datasource_ref`。
显式 `outbox.datasource_ref` 必须与之相同，冲突时配置校验返回包含这两个配置键的错误，并在 Ready
前终止启动；省略该字段或只配置轮询预算时仍扫描 client 指定库。MySQL 与 PostgreSQL 使用同一规则。
即使两个库都已具备合法 Outbox 表，也不能使用不同名称拆分写入与扫描。该规则在组件副作用与业务
Hook 之前校验，运行期候选配置也必须满足同一约束。

`enqueue_start` 的成功返回只确认事务内追加；外层事务提交前不能向调用方确认本地已受理。确认后
仍须区分三个状态：本地事实已提交、start-intent 已投递、远端 Saga 已到终态。远端已提交但收据丢失
时，dispatcher 保留原 `event_id` 重投，由远端稳定请求身份消除重复。Ready 不保证积压为零，
`napp_outbox_pending` 持续增长时须结合发布量、死信和远端查询判断，不能通过删除意图解除积压。

## 状态推进、补偿与超时

- effect_id 跨 attempt 稳定，参与方和真实副作用目标用它做业务幂等；command_id 只标识一次投递。
- Rejected 提交拒绝事实并进入冻结的逆序补偿计划。
- Unknown 进入有界 resolve，不能直接按失败补偿。
- timeout 先遵守 cancel_mode 与 TimeoutPolicy，再决定重试、resolve、补偿或人工介入。
- 不可补偿 pivot 之后不能再增加需要逆向撤销的步骤。
- 补偿失败保持补偿中或进入人工介入，不能越过事实宣称完成。
- pause、resume、retry 与 manual-close 使用可信 actor、reason、operation_id 和 expected version。
- pause/resume 初次读取时状态不允许动作属于前置条件失败；初次允许但提交 CAS 失去竞争属于可重试并发，
  gRPC 分别使用 `FAILED_PRECONDITION` 与 `ABORTED`。

timer 领取、续租、完成和交还由数据库 lease、owner、fencing token、实例版本与 timer generation 共同
约束。任一复验失配表示 worker 已失权，必须停止推进。两个副本竞争同一 result 或 timer 时，只有 Inbox、
CAS 与 fencing 胜者改变状态。

## 数据库与自举

角色 datasource_ref 必须命中 Application 冻结 catalog 中的明确 driver，不回退 default。MySQL 与
PostgreSQL wrapper 都在 Ready 前确保该角色拥有的 Saga、Inbox、Outbox 与 Catalog 结构；缺表会幂等
创建，并在同一 schema 串行权威内识别 Catalog 的直接前代结构：回填 definition 激活时间、按唯一
definition 租户补齐 capability tenant 并重算 descriptor 摘要、补齐 lifecycle operation 审计字段，
随后再创建当前对象。每个目标不变量都可从已加列未回填、已加审计列未建索引或 capability 暂时无主键的
已识别中间态独立续跑；最终门禁还要求非 candidate definition 具有激活时间。租户无法唯一推导、同名
CHECK 表达式变弱、默认值/自动更新时间、identity、
collation、索引列序或最终结构不一致都会拒绝 Ready；不会把任意漂移当作前代结构自动改写。业务表仍由
业务项目管理。

单事务 participant 使用一个 `participant.datasource_ref`。多事务域 participant 为每个
`participant.bindings.<name>.datasource_ref` 建立独立 runtime、Inbox/gate 与 Outbox dispatcher，且每个
`#[saga(binding = "...")]` descriptor 必须唯一命中一个 binding；不会选第一个 binding 或默认库。
combined 当前要求 Orchestrator 与 participant 共享同一 datasource。client 只有 reliable_start 才要求
datasource_ref。

## 协调侧服务发现

`client.orchestrator_discovery_ref`、HTTP/gRPC result 的 `orchestrator_discovery_ref` 和
`definition_catalog.registry_client.discovery_ref` 可以引用同一 `saga.discovery` 项：

~~~yaml
saga:
  discovery:
    checkout:
      service: checkout-coordinator
      service_identity: checkout-orchestrator
      address_policy_ref: coordinator-addresses
  transport:
    address_policies:
      coordinator-addresses:
        http_schemes: [http]
        http_hosts: [coordinator.internal]
        grpc_hosts: [coordinator.internal]
~~~

使用逻辑引用时启用 `nacos-discovery` 组件与实际 provider。协调者在 listener 建立后发布框架保留的
`nasa.saga.identity`、`role`、`incarnation`、`generation` 以及协议端点；HTTP 同时发布实际
`origin` 和包含 `server.context_path` 的 `base_path`。每次投递只组合一个实例的同代字段，签名覆盖
选中实例的实际路径。非健康、禁用、身份不符或地址越界的成员不参与投递；没有合法成员时保留原事件重试。
发现、连接和收据读取共享调用期限；端口与路径变化后原客户端可重新选择合法成员。

## 安全材料轮换

启用 `nacos-config` 并让固定 secret 引用从受信配置字段读取材料。`saga.credential_overlap_ms` 默认
60000，范围 1 至 3600000。配置候选必须完整构造全部已登记的 HTTP 签名器、gRPC 客户端材料、服务端
TLS acceptor 与 principal 映射后，才与配置视图一次发布；缺失引用、无效密钥、证书用途或有效期不符、
证书与私钥不匹配、authority 改变以及身份映射碰撞均拒绝整个候选，继续使用上一份有效视图。

HTTP 出站立即使用新 HMAC，入站在窗口内兼容旧 HMAC。gRPC 出站换用当前客户端证书，服务端新握手
使用当前服务证书，旧 CA 在窗口内保留。`peer_principals` 的值或 API `callers` 的 principal 键可写成
`secret://peer_certificate`，由受信证书派生 principal；固定 `sha256:...` 仍表示静态绑定。旧 principal
的接受期不超过证书实际有效期，且每个 RPC 都复验，所以既有 TLS 连接不能绕过过期撤权。最多保留八份
尚未到期的历史材料，超过上限拒绝候选，避免快速轮换扩大信任和内存。

配置发布或旧结果凭据到期会立即使旧 Catalog 执行资格失效；watch 重新验证当前 publisher、capability、
结果认证合同和全部 Ready 副本确认后恢复新建资格。参与方续租发布当前 result 凭据摘要并取得新的 route
代次，definition digest 保持不变。多个协调副本仍须完成同一安全合同的收敛；发布顺序应使生产者在
消费者的重叠窗口内切换。变更 owner、协议、payload schema 或定义步骤语义需要新的 definition 版本。

## 原始输入与 schema

HTTP start 的 `payload` 使用 `{content_type, schema_id, body}`，其中 `body` 为字节数组；gRPC 使用
`SagaPayload` 的同名字段。`content_type` 必须是规范媒体类型，非 JSON 正文必须提供非空 schema；
JSON 正文验证语法但不重写其字节。第一步 definition 和参与方 `#[saga(content_type = "...", schema_id = "...")]`
声明相同合同，任一不一致在创建或参与方业务执行前拒绝。`SagaContext::payload()` 可读取精确原始字节。

摘要覆盖规范领域身份、媒体类型、schema 和原始 bytes；经两种协议发送同一字节输入命中同一幂等事实，
字节或合同变化不会被静默吸收。原始正文随首步 execute 的持久 Outbox envelope 重投；后续步骤、resolve
和补偿按 Saga 身份读取其业务事实，不自动复制首步输入。旧 HTTP `input` JSON 入口继续可用，不能与
`payload` 同时填写；该入口先序列化为 JSON 字节，并兼容已存储的旧规范 JSON 摘要。

## 定义退休与保留引用

HTTP Registry 的 `/definitions/{tenant}/{workflow}/{version}/retire` 和 gRPC `RetireDefinition` 使用
相同的 expected seal、operation_id、reason、owner 授权及原子审计。只有 deprecated 定义可以退休；
重复操作幂等，操作身份与参数冲突拒绝。退休时在 Catalog 串行锁内保守检查该定义的全部实例（包括终态）、
迁移、审计、有效 capability 租约和 Saga Outbox 引用。已派发和已死信的 Outbox 也仍作为保留证据；
不能解析或不能确定定义归属的 Saga 行会阻止退休。

这项操作不删除 artifact，也不自动清除实例、审计或消息。保留实例时，与其关联的迟到结果及 DLT 重放
仍阻止退休；必须先按独立数据保留政策确认它们不再需要，再释放相关持久引用。对绕过产品直接删除数据库
事实、却仍保留外部待重放消息的操作，本地 Catalog 无法重新构造已删除的证据。retired 定义不再装载，
新 start 和 capability 续租拒绝；历史 artifact 仍可通过授权 Registry 查询。

## HTTP 与 gRPC 的共同 API

两个入口只处理认证、编码和标准状态投影，随后调用同一 `SagaOrchestratorApi`。实例查询支持状态集合、
workflow、创建时间上下界和有界 keyset 分页；相同过滤条件的 page token 可跨协议继续，修改过滤或伪造
游标会拒绝。管理动作共用 tenant 权限、审计原因、operation_id 和 state/control CAS。已提交操作重放
返回当前事实，不能再次产生外部命令或第二条审计。独立传输认证失败不暴露业务实例存在性。

## 观测

数据库指标覆盖实例状态、due timer、attempt、冲突、配额、Outbox 积压、派发与死信；进程指标覆盖
认证拒绝、transport 处理和监督循环。标签不包含 tenant、saga_id、business_key、payload、credential
或 datasource endpoint。

按 workflow/version 的 start-to-terminal P50/P95/P99 与样本数由已提交实例事实聚合。
`nasaga_stage_duration_seconds` 使用固定 stage 标签采集 `participant_handler`、`participant_transaction`、
`start_transaction`、`transition_transaction` 和 `timer_lateness`。前四项计入失败与取消尝试的实际占用时间；
timer lateness 在复验领取权威后采集到期后的调度滞后。Prometheus 与 OTLP 从同一有界直方图读取，
可按阶段计算 P50/P95/P99。`napp_outbox_oldest_pending_age_seconds` 使用数据库时钟查询尚未派发且未死信的
最老 Saga Outbox 行；多个受管数据库取最大值，查询失败保留上一帧并由快照年龄指标表明新鲜度。
这些指标不带 tenant、saga_id、business_key、payload、credential 或 datasource endpoint。

## 停机与恢复

停机先关闭新 Saga、管理写入口和 participant command 准入，再停止领取新 timer 与消费，排空已接管
事务和 dispatcher，最后由 Web、transport 与数据库组件反向释放。超出预算时不确认未完成消息，持久
Inbox、Outbox、instance、journal 与 timer 留给重启进程或其它副本。

Orchestrator 退出后重启会从共享数据库装载 active definition 与非终态实例，恢复 timer、Outbox 和
Catalog watcher。participant 独立重启只影响自己的 capability lease 和本地 Inbox/Outbox，不终止全局
状态机；不可用超过步骤 deadline 时，Orchestrator 仍按冻结定义收敛。

## 协议与非目标

- HTTP、gRPC、Kafka 与 Redis Streams command/result 数据面、动态 Catalog、签名 definition 发布、
  capability 续租、direct/reliable client 与精确 Outbox 唤醒均由 `napp` 按角色构造。
- gRPC 自动登记 Orchestrator/管理/Definition Registry/result/command service，自动构造出站 generated
  client，并用 mTLS principal、deadline、标准 health 与封闭收据控制 Ready 和前移。
- Kafka 使用 owner topic、broker ACK 与耐久 DLT；Redis Streams 使用同槽 stream、HMAC keyring、
  XAUTOCLAIM、XADD 收据和原子 DLT+XACK。结果不明始终保留原 event id。
- `saga_transport.proto` 与 `saga_orchestrator.proto` 随 `nasaga-runtime-core` 归档，并从同一源生成 Rust
  模块、descriptor set 与 reflection 输入。
- custom 计划仍可显式组合底层 connector；调用方一旦选择 custom，就必须拥有其生命周期、安全、路由、
  DLT、Ready 与停机边界。

任一 managed 配置缺少 datasource、身份、route、credential、DLT、consumer group、共享 replay claim、
签名 key 或 API 权限时都在 Ready 前拒绝，不允许退化到默认数据源、匿名身份、进程内临时状态机或业务
手写的第二套 Saga Server。
