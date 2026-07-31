# 快速开始

本文说明业务应用如何接入 `nasa-runtime-rust`。推荐入口是 `nasa` 门面 crate，业务项目只按需开启 feature。

## Cargo 依赖

只开启应用实际需要的 feature：

```toml
[dependencies]
nasa = { git = "https://github.com/<you>/nasa-runtime-rust", features = [
    "web",
    "mapper",
    "mapper-redis-cache",
    "tx",
    "redis",
    "cache",
    "log",
    "yml",
] }
```

如果使用已发布包：

```toml
[dependencies]
nasa = { version = "1", features = ["web", "mapper", "tx", "redis", "log", "yml"] }
```

## 最小 yml 示例

```yaml
server:
  host: 0.0.0.0
  port: 8080

log:
  level: info
  path: logs/app

mysql:
  default:
    url: ${APP_MYSQL_URL}
    max_connections: 16

redis:
  url: ${APP_REDIS_URL}
  namespace: app
  profile: RustV2

mapper:
  default_datasource: default
  cache_namespace: app:mapper

cache:
  enabled: true
  namespace: app
```

## Mapper 示例

```rust
use nasa::mapper::{Mapper, Query};

#[derive(sqlx::FromRow)]
pub struct UserRow {
    pub id: i64,
    pub name: String,
}

#[Mapper]
pub trait UserMapper {
    #[Query("select id, name from users where id = #{id}")]
    async fn find_by_id(&self, id: i64) -> sqlx::Result<Option<UserRow>>;

    #[Query(
        "select id, name from users where id = #{id}",
        cache = true,
        cache_in_tx = true
    )]
    async fn find_cached_in_tx(&self, id: i64) -> sqlx::Result<Option<UserRow>>;
}
```

## 事务示例

```rust
use nasa::tx::transactional;

#[transactional]
async fn create_order() -> anyhow::Result<()> {
    Ok(())
}
```

## 路由示例

```rust
use nasa::web::{get_mapping, mvc_router};

#[get_mapping("/health")]
async fn health() -> &'static str {
    "ok"
}

mvc_router!(());
```

## 后续阅读

| 需求 | 文档 |
| --- | --- |
| 全组件索引和 feature 列表 | `README.md` |
| Mapper SQL、动态标签、缓存、事务 | `namapper/README.md` |
| Redis 命令、pipeline、Stream、分区消费 | `nadis/README.md` |
| WebSocket 和 TCP 消息 | `naws/README.md` |
| 调度任务 | `nasched/README.md` |
| 服务发现和 REST 负载均衡 | `rest-discovery/README.md`、`rest-discovery-nacos/README.md` |
| 发布前检查 | `docs/release-checklist.md` |
