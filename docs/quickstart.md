# 快速开始

业务应用只依赖 `nasa` 门面，并按实际运行能力启用 feature。服务型项目使用
`#[nasa::application]` 统一拥有配置、组件生命周期、信号和停机顺序。

## Cargo 依赖

以下组合提供配置装载、日志、MySQL 事务、Mapper、Redis、两级缓存和 Web：

```toml
[dependencies]
nasa = { version = "1", features = [
    "application",
    "config-boot",
    "log",
    "tx",
    "mapper",
    "mapper-redis-cache",
    "redis",
    "cache",
    "web",
] }
```

使用仓库坐标时只替换依赖来源，feature 保持一致：

```toml
[dependencies]
nasa = { git = "https://github.com/nasa-runtime/nasa-runtime-rust.git", features = [
    "application", "config-boot", "log", "tx", "mapper",
    "mapper-redis-cache", "redis", "cache", "web",
] }
```

## 应用入口

```rust
mod controller;

#[nasa::application("log", "db", "redis", "cache", "web")]
async fn main(app: nasa::Application) -> anyhow::Result<()> {
    app.register(OrderService::new(app.datasource("default").await?))?;
    Ok(())
}
```

组件字符串可按任意顺序书写，宏会按规范顺序启动并严格反向停机。需要 Saga 时只声明
`#[nasa::application("saga", "web")]`；Saga 会隐式加入 DB 与 Outbox，transport 仍由业务显式选择。

## 配置

在业务进程工作目录提供 `zcf/application.yml`：

```yaml
application:
  name: order-service
  mode: service
  shutdown_timeout_ms: 15000

log:
  level: info
  path: logs/order-service

database:
  url: ${APP_MYSQL_URL}
  max_connections: 16

redis:
  url: ${APP_REDIS_URL}
  namespace: order-service
  profile: RustV2

cache:
  mode: two_level
  redis_ref: default
  cache_ttl_secs: 300
  null_ttl_secs: 30

server:
  host: 0.0.0.0
  port: 8080
  context_path: /orders
```

配置默认拒绝未知字段。数据库、Redis 和其它外部凭据通过环境变量或部署平台 secret 注入，不写入仓库。

## Mapper 与事务

```rust
use nasa::mapper::{Mapper, Query};
use nasa::tx::transactional;

#[derive(sqlx::FromRow)]
pub struct OrderRow {
    pub id: i64,
    pub state: String,
}

#[Mapper]
pub trait OrderMapper {
    #[Query("select id, state from orders where id = #{id}")]
    async fn find_by_id(&self, id: i64) -> sqlx::Result<Option<OrderRow>>;
}

#[transactional]
async fn create_order() -> anyhow::Result<()> {
    insert_order().await?;
    Ok(())
}
```

参与事务的写入必须使用当前 ambient datasource。需要可靠发布外部事件时使用 Outbox；消费消息并与
本地业务写共同提交时使用 Inbox。

## 路由

```rust
use nasa::web::get_mapping;

#[get_mapping("/health")]
async fn health() -> &'static str {
    "ok"
}
```

声明 `web` 后运行时提供统一监听、readiness、排空和停机。业务路由、数据源和后台任务应从
`nasa::Application` 取得受管能力，不自行复制生命周期。

## 后续阅读

| 需求 | 文档 |
| --- | --- |
| 全组件索引和 feature 列表 | [根 README](../README.md) |
| Application 组件字符串与扩展点 | [napp README](../napp/README.md) |
| Saga 生产接线 | [Saga 生产运行指南](saga-production.md) |
| 部署与停机 | [应用部署指南](deployment.md) |
| 运行期观测与故障处理 | [应用运维指南](operations.md) |
