# 应用运维指南

本文说明应用模式进程的日常观测、配置刷新、停机语义和故障处置。业务端口、下游依赖和数据恢复
流程由对应项目手册补充。

## 运行状态

```text
Bootstrap → Starting → UserHook → Ready → Running → Stopping → Stopped
                                                        └────→ Failed
```

- `Ready` 只在组件启动、业务 Hook 成功、资源封存和必要 listener 绑定后提交。
- 任一关键任务在 Running 阶段意外结束会提交失败意图，进程进入统一停机。
- 进入 Stopping 后 readiness 先变为 false，再摘流、停止 accept、排空任务并反向释放资源。
- `Failed` 是终态，不会回到 Running。

## 退出码

| 场景 | 结果 |
| --- | --- |
| 常驻服务收到终止信号并在预算内清理完成 | `0` |
| 启动失败或关键任务先发生故障 | 非零 |
| 批处理尚未完成时收到信号 | `128 + signal` |
| 批处理业务结果已提交后在清理阶段强退 | `0` |
| Stopping 阶段再次收到终止信号 | 立即退出，按已提交终止意图决定 |

监督器同时记录退出码和停机前的 primary error。清理阶段的后续错误进入 shutdown report，不覆盖更早
提交的根因。

## 日常检查

- 应用名称、模式和最终配置修订符合部署批次。
- `/readyz`、`/healthz` 与真实 listener 状态一致。
- 服务发现注册地址与实际监听地址一致。
- 关键任务没有提前退出，后台任务数量和队列长度没有持续增长。
- 数据库池、缓存、配置监听和服务发现处于最后一次成功状态。
- 配置刷新没有 `ApplyFailed` 或 `RestartRequired` 长时间未处理。
- 日志中没有连接串、访问令牌、业务 payload 或控制 token。

## 配置刷新

配置视图把期望快照和每个组件的应用状态放在同一次发布动作中。运维必须同时查看配置修订与状态：

- `Applied`：组件已应用目标快照；
- `ApplyFailed`：目标快照已更新，组件仍运行在最后一次成功状态；
- `RestartRequired`：字段不能热切，需要滚动重启。

远端配置不能动态改变进程身份、模式、线程数或全局 deadline。启动阶段发生冲突时拒绝 Ready，运行期
冲突标记为需要重启。候选快照失败不得撤销当前可用资源。

## 停机与超时

正常停机只发送一次 SIGTERM，然后等待进程自行退出：

```text
NotReady / 摘流
  → 停止 listener 接收
  → 排空 Web 与用户任务
  → 关闭业务托管资源
  → 关闭数据库、缓存和配置监听
  → 刷新日志并退出
```

所有异步清理共享一个绝对截止时间。超时后运行时记录未完成阶段并中止可取消任务；不让出执行权的
代码或无法取消的阻塞工作仍由部署平台的强制终止上限兜底。

## 常见启动故障

| 现象 | 优先检查 |
| --- | --- |
| 找不到主配置 | 工作目录与 `zcf/application.yml` 是否随产物部署 |
| 无 Web 后进程在 Hook 返回后退出 | `application.mode` 是否显式为 `service` |
| 配置反序列化失败 | 环境键层级、字段类型和冲突键 |
| 声明组件但提示 feature 缺失 | 业务 manifest 是否开启对应门面 feature |
| listener 启动超时 | 地址、端口和业务就绪屏障 |
| 数据库或缓存失败 | 脱敏目标主机、端口、模式和超时 |
| Ready 后很快退出 | 首个 critical task 错误 |
| SIGTERM 后超过预算 | 未 join 任务、长期资源借用、无限流式响应或阻塞析构 |

## Saga 值班

先查看 `nasaga_manual_intervention`、`nasaga_waiting_resolution`、`nasaga_due_timer`、
`nasaga_conflict_total`、`nasaga_kafka_command_*` 和 `nasaga_kafka_result_*`，再关联 `nafka` 的
retry、DLT 与 commit 指标。

- `authentication_failed_total` 上升：核对凭据、时钟和报文完整性；
- `replay_rejected_total` 上升：追踪重复来源，不清空仍有效 nonce；
- `capacity_rejected_total` 上升：隔离异常 producer/path，并核对该信任边的容量预算；
- `nasaga_http_command_dlt_total` 上升：核对原 envelope、冻结定义摘要和部署快照；
- Unknown 积压：检查 typed resolver、外部事实和 resolution budget，不能手改状态；
- timer fencing 丢失：旧 worker 已失权，停止推进，由新 owner 重新领取。

人工恢复先读取 attempt、迁移、控制、管理和冲突审计，再沿冻结计划使用稳定
`operation_id/effect_id` 操作。禁止删除 Inbox、participant gate 或 DLT 来消除告警。

部署边界见 [应用部署指南](deployment.md)，Saga 细节见 [Saga 生产运行指南](saga-production.md)。
