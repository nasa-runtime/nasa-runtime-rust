# 应用部署指南

本文约束使用 `#[nasa::application]` 的业务进程如何构建、配置、接流和停机。保留独立入口的项目继续
按自身启动协议部署。

## 构建

在业务工程根目录使用锁文件生成发布二进制：

```bash
cargo build --locked --release
```

工作区中的单个二进制使用 package 名选择：

```bash
cargo build --locked --release -p <package>
```

镜像只复制最终二进制、`zcf/` 下的配置和业务必需的证书或静态资源。进程工作目录必须能解析
`zcf/application.yml`；该文件必须存在，内容可以是 `{}`。

## 启动配置

```yaml
application:
  name: order-service
  mode: service
  startup_timeout_ms: 30000
  shutdown_timeout_ms: 15000
```

- `mode: service` 用于常驻进程；无 Web 的后台服务必须显式设置。
- `mode: batch` 用于任务完成后正常退出的批处理。
- `mode: auto` 只适合声明了 Web、长连接、服务发现或调度组件的应用。
- `startup_timeout_ms` 约束组件与业务 Hook 启动。
- `shutdown_timeout_ms` 是全部反向清理共享的总预算。

配置优先级从低到高为主文件、显式 profile、远端 overlay 和 `APP__...` 环境覆盖。凭据只通过部署
平台的 secret 注入能力提供，不写入配置文件、镜像层、命令历史或普通日志。

## 容器与进程监督

容器入口使用 exec 形式，让业务进程直接成为容器主进程并收到 SIGTERM：

```dockerfile
ENTRYPOINT ["/app/order-service"]
```

部署平台的强制终止宽限期应大于 `application.shutdown_timeout_ms`，并预留摘流、日志刷新和调度抖动。
首次终止信号触发正常停机；Stopping 阶段再次收到终止信号会立即退出。监督器依据退出码和 Ready 状态
决定重启，不能只判断端口是否存在。

## 健康端点

声明 `web` 组件且 `server.health=true` 时，运行时提供：

- `<context_path>/healthz`：进程存活状态；
- `<context_path>/readyz`：业务接流状态。

readiness 是负载均衡和滚动部署的接流条件。自行在 UserHook 中托管 HTTP 服务的项目不会自动获得
这些端点，必须提供自己的管理入口或等价健康信号。

`server.port: 0` 的真实端口在 bind 后产生，可通过应用运行时的监听地址能力读取；需要固定服务端口的
部署不应使用该设置。

## 部署顺序

1. 使用锁文件在受控依赖源中完成构建。
2. 确认生产配置包含正确的应用模式、超时、监听地址、组件开关和 secret 引用。
3. 完成数据库扩展、外部权限和下游资源准备。
4. 先启动少量实例，确认 Ready、配置修订、下游连接和错误率，再扩大批次。
5. 回退时仍发送 SIGTERM 并等待摘流、任务排空和资源释放。

具体条件见 [交付就绪清单](release-checklist.md)，运行期处置见 [应用运维指南](operations.md)。

## Saga 部署

Saga 采用 expand-first：先按迁移清单扩展结构并核对历史行，再滚动启动新 binary。只有旧 binary 已
完全退出、审计已导出且 replay horizon 允许时，才执行结构回退。

Saga binary collation 脚本没有通用 down。执行前记录原 collation，并单独评估大表重建、metadata
lock、复制延迟、磁盘余量和完成时间。Ready 前完成 `Orchestrator::verify_startup`；任何活跃定义或
descriptor 漂移都拒绝接流。

本地容器能够确认 MySQL 提升、Kafka 多 broker、ACL、消息重投和故障恢复语义，但不能替代生产网络、
磁盘、容量和灾难恢复批准。完整边界见 [Saga 生产运行指南](saga-production.md)。
