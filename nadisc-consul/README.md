# nadisc-consul

供任意 Rust 微服务复用的 Consul adapter：按显式 datacenter 和服务别名发现实例，向固定 agent 注册由本 owner 独占的服务与 HTTP/TTL 检查。与 `nadisc-runtime` 配合管理重试、资格撤销和有界注销，无需 Saga 或 napp。

## 工作方式

查询使用一致性 Health API，保留节点 ID 与 Service ID。同一 IP/端口的不同记录不会合并。必须存在业务服务检查与节点检查，且全部 passing，才是健康候选；维护状态、warning、critical、缺少业务检查、passing weight 为零均不接流。HTTP 200 的 ACL 裁剪仍视为 `PermissionFiltered`，不得当作完整空集。

未设置 metadata 的服务可被正常发现；`Meta` 缺席、`null` 和空对象均视为空映射。错误类型、非字符串值和含混的编码键仍会使整份查询失败，不跳过成员拼出部分集合。

写入只访问显式 `agent_addr`，冻结 `/agent/self` 的 NodeID 与 datacenter；目标变化时拒绝继续写入。服务 ID 由 owner UUID 与 registration 名组成；不会接管共享 ID。框架 metadata 编码为 Consul 接受的键并在查询时还原，`nasa.consul.*` 标识由 adapter 产生。

自注册回查按冻结 agent 的 NodeID 与 Service ID 选择目录条目，再核对配置服务名、owner 和自有检查 `<service-id>-health` 的归属。其他节点的同 ID 记录不影响本注册就绪。agent 在册但目录或自有检查缺席时保持不健康；重复身份、owner 不匹配或检查归属含混返回 Conflict，不按数组顺序挑选健康记录。

自有检查在固定 agent 和健康目录中的 Type 都必须匹配冻结的 TTL/HTTP 模式。HTTP 检查还须将 agent 顶层 Interval/Timeout 规范化为精确时长，与冻结周期和超时分别核对。类型或时限不符返回 Conflict，旧 passing 不能产生新的注册健康期限或满足启动确认；时限缺失或非法也不能提供新的健康证据。恢复原定义后可重新取证，无需重新注册服务；显式 HTTP 模式仍由 agent 探测。该核对不能证明不可读的完整 TTL 时长或 HTTP URL，也不提供跨查询的原子性和读写 CAS。

agent 在册记录和健康目录的 Address/Port 都必须与冻结注册计划一致，任一侧变化即返回 Conflict，撤销该注册的就绪证据。普通发现继续反映目录中的实际端点；自注册不会通过重新注册覆盖管理端变化。

自注册健康还要求固定 agent 的自有检查 Status 为 passing，然后继续核对目录归属、维护状态及全部服务与节点检查。agent 明确报告 critical 或 warning 时立即判为不健康，即使目录仍为 passing 也不能保留注册就绪。Status 缺席、null、未知值或错误类型均不能提供新的健康期限；已有证据仅按公共 owner 的错误宽限到期。状态判断不构成 TTL 写入权限：归属、定义与宿主资格仍成立时，已到期的 critical 检查可以续约恢复。agent 与目录的读取仍不构成原子快照。

```yaml
discovery:
  enabled: true
  eligibility_probe: true
  connections:
    primary:
      provider: consul
      consul:
        api_addr: https://consul.internal:8501
        agent_addr: https://local-agent.internal:8501
        datacenter: primary
        token: ${CONSUL_TOKEN}
        services:
          orders: order-api
  registrations:
    http:
      qualifier: primary
      service_name: orders
      listener_ref: web
      ip: ${REGISTER_IP}
      consul:
        check:
          mode: http
          url: http://order-host:8080/discovery/eligible/http
          interval_ms: 5000
          timeout_ms: 1000
```

连接名 `primary` 提供默认选择；需要其他环境时新增具名连接，并用 `qualifier` 精确选择。
`datacenter` 是 Consul 查询作用域，不是 qualifier；注册仍只写配置的 agent。REST Mapping
使用 `Client::with_clients(app.discovery()?.rest_clients()?)`，详见[调用指南](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/discovery.md)。

本例使用 `napp` 的 `discovery` 和 `web` 组件；独立宿主实现等价的本地资格端点。HTTP 检查必须由 agent 可达，失败真实返回 503，不能用统一 HTTP 200 的业务错误包装；不应指向包含自身注册结果的整体 readyz。

TTL 检查替换为：

```yaml
check:
  mode: ttl
  ttl_ms: 15000
  report_interval_ms: 5000
```

TTL 上报取决于宿主当前资格，初始 critical，不以进程存活冒充业务就绪。期限必须覆盖上报间隔、请求预算与余量。不配置 agent 时只读；只读连接不需要写侧权限。

TTL 模式声明同身份、同健康状态的幂等续约能力。passing 响应丢失后，owner 可在退避后重复同一自有检查的 passing，并在提交处重新核验资格；不会重建服务、替换检查或清除维护状态。新的成功响应不证明更早请求已经结束，此类未决写入在后续注销时仍形成残留报告。HTTP 检查不启用主动续约。

每次 TTL 上报先读取固定 agent 的精确 CheckID，核对 ServiceID、ServiceName 与 TTL 类型；缺席、改绑或类型变化均拒绝写入。单纯 TTL 到期仍允许在归属和宿主资格成立时恢复。Consul 的检查查询不回传 TTL 时长，且查询与状态更新之间没有 CAS，无法证明完整检查定义未被外部改写，也无法排除读写之间的管理端竞态。部署必须限制框架 Service ID 与 CheckID 的外部写权限；metadata 和检查名称不构成此类互斥保证。

## 配置与限制

`ConsulConfig` 显式提供 `api_addr`、`datacenter`、非空 `services`；`agent_addr` 可省略；`token` 默认空，`ca_pem` 可配置显式 PEM。请求不跟随重定向，不读取环境代理，不跳过 TLS 验证。显式 CA 替代默认根，凭据在请求头中发送。

默认 `request_timeout_ms=3000`、`blocking_wait_ms=10000`、`max_response_bytes=4194304`、`max_instances=4096`。blocking 等待有独立有界预算，不长期占用普通查询并发；索引变化只触发完整重查，不能给旧事实续期。公共 owner 还会按观察周期复验。

查询身份至少需要绑定服务与节点的 read 权限；写身份还需要 agent read、自身服务写以及检查更新权限。ACL 失效立即撤销该连接下旧观察，不借用其他连接的 token。

配置冻结在 owner 周期内，修改 token 来源、agent、datacenter 或服务绑定要求重建。agent 记录丢失和管理端删除可能无法区分，因此不会盲目重建缺失记录；运维先确认管理意图后重启 owner。服务注销会删除该服务关联检查，不能把框架服务 ID 当作共享检查容器。Consul 不提供数据库级 fencing；不可达删除和结果不明写入必须检查 owner 的残留报告。

[公共生命周期与手动宿主用法](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nadisc-runtime/README.md)。

## License

MIT OR Apache-2.0。
