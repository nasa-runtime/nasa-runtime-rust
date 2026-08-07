# 安全策略

## 支持范围

安全修复覆盖当前受保护分支上的全部稳定组件。标记为 experimental 的能力同样接受漏洞报告，但在
晋升前不承诺稳定 API。具体源码状态和修复沿革以 Git 提交记录与安全公告为准。

## 漏洞报告

发现安全问题时，请使用仓库的
[Private vulnerability reporting](https://github.com/nasa-runtime/nasa-runtime-rust/security/advisories/new)
私密提交。

不要在修复可用前公开漏洞细节。若私密入口暂时不可用，只创建不含技术细节、凭据、复现路径或受
影响目标的普通 issue，请求维护者恢复私密报告入口。

安全敏感问题包括：

- WebSocket、REST、调度或管理入口的认证、授权或租户隔离绕过；
- 日志、指标、错误、示例或 README 泄露密钥、token、连接串或业务 payload；
- mapper 宏产生 SQL 注入或不受控动态片段；
- 缓存一致性导致跨租户串读或把未提交数据发布到共享缓存；
- 事务与 after-commit 顺序错误，导致外部副作用先于业务事实提交；
- Redis、Kafka、Stream 或服务发现把数据发送到错误 key、group、topic 或实例；
- 无界队列、任务、缓冲区、payload、图片尺寸或连接数造成拒绝服务；
- Saga 身份、participant gate、管理权限、timer fencing、Unknown 裁决或 DLT 顺序被绕过。

## 安全默认值

- Mapper SQL 使用绑定参数；原始 SQL 片段只通过显式白名单能力进入。
- 事务内共享缓存行为必须显式声明，避免未提交视图泄露到 L2。
- WebSocket 配置包含连接总量、未认证连接、payload、发送队列和 handler 并发上限。
- Redis Stream、Kafka 和分区消费错误必须对调用方可见，不能静默丢弃。
- 配置遇到未知字段、非法必填值或冲突组合时快速失败，并对凭据脱敏。
- 兼容密码能力只用于受控迁移；新业务使用现代认证加密入口。
- 历史私钥解密路径默认不进入稳定能力组合，启用时还需要运行期风险控制。
- Saga command/result producer 来自 broker ACL、mTLS principal 或覆盖完整 envelope 的消息签名，
  不能相信 payload 自报身份。
- Saga 管理 actor 与权限来自 JWT、mTLS 或等价受信上下文，正文不能覆盖。
- Participant 授权精确绑定 `(workflow, definition_version, digest)`、步骤与 Orchestrator；禁止跨业务域
  扁平白名单。
- 确定性 command 拒绝使用 runtime 的封闭原因码映射，并先持久化 DLT，再推进源 Outbox 或 offset。
- HTTP 类入口按 producer/path 隔离 authenticator、replay guard、容量和指标；多副本使用共享强一致
  nonce claim。
- Unknown 只由声明 typed resolve 能力的步骤使用；超时或“查无记录”不能伪造成确定失败。
- timer owner 逐副本唯一且重启稳定；fencing capability 同时绑定运行实例随机 nonce，不接受裸字符串
  构造，也不能跨领取批次复用。

## 依赖策略

供应链检查使用：

```bash
cargo deny check
```

临时忽略安全公告时必须说明依赖保留原因、受影响路径是否可达、现有隔离措施以及移除忽略项的明确
条件。高风险路径不能仅依赖文档告警，应通过 feature、运行期开关、权限边界或代码删除降低可达性。
