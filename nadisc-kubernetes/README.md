# nadisc-kubernetes

任意 Rust 微服务可使用的 Kubernetes 只读服务发现 adapter。显式选择集群、namespace、Service 与端口，从 Service 和 EndpointSlice 的联合证据得到当前可接流的端点；与 `nadisc-runtime` 共用观察、期限和关闭管理，无需 Saga 或 napp。

## 架构与边界

每次完整观察读取 Service、收齐分页 EndpointSlice，再复验 Service 的 UID 和 resourceVersion。仅接纳当前 Service 的 controller-owned Slice；同名 Service 重建不会复活旧 Slice。端点使用 Slice 的实际端口，不把 Service port 当成 Pod port。保留 targetRef UID，地址被新 Pod 复用时撤销旧候选。重复地址不会增加权重，冲突对象身份会失去健康资格。

查询身份由规范化 API server、namespace、Service 名、命名端口及地址族组成，本地别名不改变身份。快照中的实例身份保留 API server、Service UID、targetRef UID、地址族、地址、实际端口及 TCP 协议。缺少 targetRef UID 时明确采用 Service 代次内的地址身份；同名 Service 重建仍会改变身份，但不能识别同一 Service 代次内未提供 UID 的目标对象地址复用。身份键不包含凭据。

watch 只触发完整重读。BOOKMARK 不续健康期限；断流、过期 resourceVersion、分页失败或容量超限不能发布部分成功。周期复验保证没有事件时证据仍会更新或过期。条件必须明确 ready，serving 为 false 或 terminating 为 true 不接纳；`publishNotReadyAddresses` 无法当作真实业务就绪。

控制器生成的合法空 Slice 可将 endpoints/ports 表达为 null 或空数组。完成 Service 复验与全部分页读取后，归属有效的空 Slice 不要求提供 TCP 端口；完整空集合立即撤销旧候选，不使用错误宽限。非空端点仍要求唯一的命名 TCP 端口；缺失字段、错误类型和分页不完整继续视为不完整响应。

完整复验确认 Service 的命名 TCP 端口被移除、改为其他协议或产生歧义，或 Service 不再满足当前身份与受支持来源条件时，返回 `InvalidSnapshot` 并立即撤销该目标旧路由，即使配置了 `stale_if_error_ms` 也不能继续提交。单纯请求失败、分页不完整或同 UID 的 resourceVersion 在读取期间变化仍遵守公共 runtime 的固定错误宽限；它们不能延长旧证据，也不能重新开放已撤销的路由。

不读取 Pod、Node 或 Secret，不注册实例，不支持 ExternalName、无 selector 的手工服务或任意手工 EndpointSlice。服务选择器和 namespace 配置不是 RBAC 权限隔离的替代品。DNS 模式使用公共 runtime 的 DNS provider，不能将 DNS 解析成功视为 Pod 健康。

## 配置

启用 `nadisc-runtime/kubernetes`，或 NASA 的 `application`、`discovery-kubernetes`，使用 `#[nasa::application("discovery")]`。

```yaml
discovery:
  enabled: true
  connections:
    primary:
      provider: kubernetes
      poll_interval_ms: 5000
      kubernetes:
        auth:
          mode: in_cluster
        namespaces: [payments]
        services:
          orders:
            namespace: payments
            name: order-api
            port_name: http
            address_family: both
        watch_timeout_ms: 30000
        page_size: 200
```

`primary` 是默认连接名。多个集群分别配置连接与凭据，REST Mapping 用 `qualifier` 选择连接，
`service` 使用该连接内的别名；不会因目标无实例而转到其他集群。使用 Mapping 时额外启用
`rest-client`，通过 `Client::with_clients(app.discovery()?.rest_clients()?)` 注入。

集群外使用显式来源：

```yaml
auth:
  mode: kubeconfig
  path: /etc/kubernetes/client.yml
  context: payments-reader
```

不搜索用户默认 kubeconfig，不执行 exec/auth-provider，不使用 impersonation、环境代理、跳过 TLS 验证或隐式认证回退。kubeconfig 支持 token、tokenFile 或客户端证书/私钥，三者只选一种；CA 必须明确配置。路径相对 kubeconfig 所在目录解析。集群内使用标准 ServiceAccount token 与 CA 挂载；既定 tokenFile 每次请求重新读取，支持轮换，文件须是最多一 MiB 的普通文件。认证材料不进入诊断。

运行期 tokenFile 缺失、不可读、变成非普通文件、超过容量或读取期间增长越界，以及 token 为空、编码非法或不能形成合法认证头，统一作为 `Unauthenticated` 撤销本连接全部目标的旧发现资格。恢复合法文件后，需要新的完整观察才能重新接流，不沿用先前身份的证据；其他连接保持独立。启动时 kubeconfig、CA 与静态绑定的配置校验仍按各自配置语义报告错误。

默认 `request_timeout_ms=3000`、`watch_timeout_ms=30000`、`max_response_bytes=4194304`、`max_instances=4096`、`page_size=250`。`address_family` 为 `ipv4`、`ipv6` 或默认 `both`；协议仅 TCP，必须命中明确端口名。其余连接时效由公共 runtime 配置，不另加 watcher 缓存宽限。配置与认证来源变更要求重启，既定 token 文件轮换除外。

## namespace 最小读取权限

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: service-discovery-reader
  namespace: payments
rules:
  - apiGroups: [""]
    resources: [services]
    verbs: [get, list, watch]
  - apiGroups: [discovery.k8s.io]
    resources: [endpointslices]
    verbs: [get, list, watch]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: service-discovery-reader
  namespace: payments
subjects:
  - kind: ServiceAccount
    name: order-client
    namespace: payments
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: service-discovery-reader
```

调用 `app.discovery()?.client(Some("primary"))` 并观察 `orders`。REST 注入该连接客户端；gRPC 对未携带协议 metadata 的端点使用显式 `DiscoveryProtocol` 指定可信 authority/TLS，不能从 port name 猜证书身份。Saga 的严格身份合同不会从 Kubernetes 就绪条件自动生成。

401/403 会关闭当前身份下的旧路由；用户持有的旧快照没有持续发送权。监控公共 owner 的错误类别、证据年龄和快照容量，权限恢复后以新的完整观察重新取得资格。

[公共 owner 与调用合同](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nadisc-runtime/README.md)。

## License

MIT OR Apache-2.0。
