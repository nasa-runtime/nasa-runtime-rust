# 受管协议宿主与资源边界

[English](protocol-hosting.en.md)

应用可以保留自己的握手、信封和授权模型，同时把监听、连接容量、首次认证期限与退出等待交给 NASA Runtime。
内置 endpoint 消息路径与自定义协议路径分别装配，不把协议接管误认为自动取得内置会话或集群能力。

## 组件职责

| 组件 | 提供的能力 | 应用仍须负责 |
| --- | --- | --- |
| napp | feature 与配置校验、统一启动许可、组件监督和有序关闭 | 登记协议处理器、声明关键依赖与业务初始化 |
| naws | listener、连接与未认证名额、原始连接上下文、任务排空 | 身份、路由、订阅、协议错误与消息权限 |
| naws-proto | typed codec、不可变动态 schema 计划、受限 JSON 转换 | 双方 schema 一致、业务字段和绝对时间校验 |
| nafka / nadis | 受管来源、输入责任、重试与后端确认 | 消息身份、去重、业务副作用及投递结果 |
| nafana | 有限指标目录、共享资源采样、独立出口 | 部署可见范围、固定标签目录与告警含义 |

## 接入顺序

业务开启 nasa 的 `application` 与 `ws` feature，声明 `"ws"` 组件，在 UserHook 调用
`Application::configure_ws`，通过 `ServerBuilder::protocol_handler` 登记拥有整条连接的异步处理器。
处理器收到 `tokio::net::TcpStream` 和 `naws::server::ProtocolContext`。Ready 前完成装配，
宿主放行后才接纳连接；登记成功不等于已经绑定端口或开放业务。

独立宿主可使用 `bind_suspended`，已有 listener 则使用 `bind_suspended_on` 移交其所有权。
后者不再次 bind，不接受另一个 `ws_addr`；宿主完成准备后调用 `activate()`。
外部可达地址、动态路由目录和端口映射仍须由应用或部署维护。

自定义处理器不执行内置 Message、endpoint、bridge、relay，也不能同时配置内置集群 transport。
`websocket_only()` 只说明连接来自独立 WebSocket 监听器，不能作为路由授权；
HTTP Upgrade 和原始帧分类由处理器及其选定的传输适配器完成。

## 认证与关闭不变量

`auth_deadline()` 在 accept 时固定。协议分类、Upgrade 和异步授权使用同一个截止点，
不能在每个阶段重新计时。`authenticated()` 只在期限内首次确认且未取消时成功；
它归还未认证名额并通知连接 owner，不创建用户身份，也不续期订阅或业务许可。
期限内的成功确认可以被稍后调度的 owner 观察，服务取消仍优先终止连接。

`peer_addr()` 是真实 TCP 对端，代理身份需要另行信任与验证。应用通过 `cancellation()`
观察连接关闭，并通过 `spawn` 登记子任务。上下文退出会发出取消，但服务仍等待已登记任务的真实退出。
不得把异步等待取消、父 future 返回或关闭超时当作 writer 已停止的证明。

## 有界正文与动态 schema

`transport::Connection::accept_with_budget` 在读入或分配前取得共享字节责任；传输缓冲、
导出的正文和共享切片保留责任，直到最后一个 owner 释放。预算不等于整个进程 RSS 上限，
HTTP、MQ、WebSocket 掩码、TLS 与内核仍可能复制。

`dynamic::SchemaPlan` 从封闭目录编译固定模式，限制消息字节、展开字段、深度及动态值数量。
计划支持 JSON_BYTES、VARINT_TLV、BITPACK_TLV 与 FAST_FIXED；后者只接受不可空的定宽基础字段。
typed derive 与 dynamic 计划的类型集合不同，不能将一个入口的 JSON 映射隐式套给另一个入口。
目录编译不加载任意代码，也不授权消息来源或目标。

## 配置与观测

声明式应用的主配置由非空 `APP_CONFIG_FILE` 指定；未设置或为空时使用 `zcf/application.yml`。
路径相对进程工作目录解析，指定文件无效时拒绝启动。日志路径和网络出口分别显式配置；
具体产品可提供自己的日志环境变量。环境快照固定于启动，配置观察到新文件不表示资源已生效。

指标目录在 Ready 前登记；状态标签可能迟到出现的来源使用最坏序列数预留，容量不足拒绝登记。
资源采样仅读取当前进程及配置的可见 cgroup、文件系统、网卡和设备；缺失、失败、不支持与过期
使用不同状态，不能补成健康零值。指标与抓取就绪不能替代产品的租约或写出许可。

socket-center 在这些通用能力上实现路由定义、授权、订阅、实时下行及集群控制。
其协议、路由许可和业务投递合同属于产品；应用不能因使用同一基础框架便假定兼容同一服务端合同。

[naws](../naws/README.md) · [ProtocolBytes](../naws-proto/README.md) · [napp](../napp/README.md) ·
[资源观测](../nafana/OBSERVABILITY.md) · [受管能力](managed-capabilities.md)
