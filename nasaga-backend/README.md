# nasaga-backend

`nasaga-backend` 定义 MySQL 与 PostgreSQL Saga store 共用的持久行模型、封闭结果和分组异步能力合同。
它不持有数据库连接，不依赖 SQLx driver，也不负责建表或 migration。

## 持久合同架构

运行核心按职责依赖 `SagaInstanceStore`、`SagaJournalStore`、`SagaTimerStore`、
`SagaParticipantStore`、`SagaAuditStore` 与 `SagaGovernanceStore`，不会把全部持久操作压进单个巨型 trait。
具体 adapter 负责把数据库错误收敛为 `SagaBackendErrorKind`，状态机不解析 SQLSTATE 或错误文本。

timer fencing capability 由本 crate 发行并保持不可复制。adapter 只有在数据库原子取得或接管租约后，
才能把 capability 与实际领取行组合为 `TimerClaimBatch`；旧 token 的完成、交还和推进必须由条件写拒绝。

## 主要边界

本 crate 不读取配置，也不承诺跨数据库事务。transaction runner、Inbox、Outbox、datasource 与 driver
身份由运行核心的 `SagaBackend` 组合合同绑定，避免只替换 store 却把原子链的其它部分留在错误后端。
