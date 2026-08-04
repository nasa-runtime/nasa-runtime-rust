# 应用部署指南

本文约束使用 `#[nasa::application]` 的业务进程如何构建和部署。保留独立入口的项目继续按自身
启动协议部署，不需要为了升级运行时而改成应用模式。

## 构建

在业务工程根目录使用锁文件生成发布二进制：

```bash
cargo build --locked --release
```

工作区中的单个二进制使用 package 名选择：

```bash
cargo build --locked --release -p <package>
```

发布镜像只需要复制最终二进制、`zcf/` 下的版本化配置和业务必需的证书或静态资源。
进程工作目录必须能解析 `zcf/application.yml`；该文件必须存在，内容可以是 `{}`。

## 启动配置

每个应用模式进程至少明确以下配置：

```yaml
application:
  name: order-service
  mode: service
  startup_timeout_ms: 30000
  shutdown_timeout_ms: 15000
```

- `mode: service` 用于常驻进程。无 Web 的后台服务必须显式设置，不能依赖任务数量推断。
- `mode: batch` 用于任务完成后正常退出的批处理。
- `mode: auto` 只适合声明了 Web、长连接、服务发现或调度组件的应用；解析结果在启动期固定。
- `startup_timeout_ms` 约束异步组件与业务 Hook 启动，不覆盖最初的配置文件读取和 runtime 构造。
- `shutdown_timeout_ms` 是全部反向清理共享的总预算，不是每个组件各自拥有一份预算。

配置优先级从低到高为主文件、显式 profile、远端 overlay、`APP__...` 环境覆盖。示例：

```bash
APP_PROFILE=prod \
APP__SERVER__PORT=8080 \
APP__REDIS__PASSWORD="$REDIS_PASSWORD" \
./target/release/order-service
```

环境覆盖会沿用主配置已声明的标量类型：主配置中的字符串字段即使收到全数字文本也仍是字符串，
端口和开关仍分别解析为数值与布尔值。凭据只通过部署平台的秘密注入能力提供，不写入版本化配置、
镜像层、命令历史或普通日志。

## 容器与进程监督

容器入口必须使用 exec 形式，让业务进程直接成为容器主进程并收到 SIGTERM：

```dockerfile
ENTRYPOINT ["/app/order-service"]
```

部署平台的强制终止宽限期应大于 `application.shutdown_timeout_ms`，并额外预留摘流、日志刷新和调度抖动。
建议至少满足：

```text
termination grace >= shutdown timeout + 5 seconds
```

首次 Ctrl-C 或 SIGTERM 会触发常驻服务的正常停机；进入 Stopping 后再次收到终止信号会立即强退。
外部监督器应根据进程退出码决定重启，不以“端口仍存在”代替进程与就绪状态检查。

## 探针

声明 `web` 组件且 `server.health=true` 时，运行时提供：

- `<context_path>/healthz`：进程存活探针。
- `<context_path>/readyz`：流量就绪探针。

readiness 应作为负载均衡和滚动发布的接流条件；liveness 的失败阈值应允许短时调度抖动。自行在
UserHook 中托管 HTTP 服务的项目不自动获得这些端点，必须使用项目自己的管理面或端口探针。

使用 `server.port: 0` 时，真实端口只在 bind 后产生，可通过应用运行时的监听地址能力读取；这种配置
适合临时验收，不适合需要固定服务端口的常规部署。

## 发布顺序

一次安全发布至少执行以下步骤：

1. 使用锁文件离线或在受控依赖源中完成发布构建。
2. 运行离线合同检查、目标项目编译检查和集中真实进程验收。
3. 校验生产配置包含正确的 `application.mode`、超时、监听地址和组件开关。
4. 先发布单实例，确认 Ready、配置版本、下游连接和错误率，再扩大批次。
5. 回滚时仍发送 SIGTERM 并等待反向清理，不直接跳过摘流和连接排空。

完整发布门禁见 [Release Checklist](release-checklist.md)，运行期处置见 [Operations](operations.md)。
