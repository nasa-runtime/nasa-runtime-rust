# nacache-macro

`nacache-macro` 提供 `#[cached]` 和 `#[cache_invalidate]` 属性宏。业务通常从 `nasa::cache` 使用。

```toml
[dependencies]
nasa = { version = "1", features = ["application", "cache", "redis"] }
```

```rust
use nasa::cache::{cached, cache_invalidate};
```

## `#[cached]`

```rust
#[cached(scene = "user", key = "user:{id}", refresh_ms = 1000, expire_ms = 3000)]
async fn find_user(self: std::sync::Arc<Self>, id: i64) -> anyhow::Result<User> {
    self.query_user(id).await
}
```

宏展开流程：

1. 用 `format!` 根据 key 模板拼缓存 key。
2. 调 `cacheable::get_or_load_2level`。
3. L1 命中直接返回；L1 miss 进入 L2；L2 miss 执行原函数体。

## `#[cache_invalidate]`

```rust
#[cache_invalidate(scene = "user", key = "user:{id}", value = "User")]
async fn update_user(&self, id: i64, user: User) -> anyhow::Result<()> {
    self.update_db(id, user).await
}
```

宏展开流程：

1. 先拼 key。
2. 执行原函数体。
3. 写操作完成后调用 `cacheable::invalidate::<V>` 删除 L2 和本节点 L1，并按已安装的运行时发布
   跨实例失效消息。

失效失败只记录告警，不改变原函数返回值。

## YML 配置与使用

`nacache-macro` 没有运行期 yml。缓存 key、scene、刷新窗口和过期时间都写在属性宏上；Redis 连接、L1 容量和默认 TTL 由 `cacheable` 的应用配置负责。

属性示例：

```rust
#[cached(scene = "user", key = "user:{id}", refresh_ms = 1000, expire_ms = 3000)]
async fn find_user(self: std::sync::Arc<Self>, id: i64) -> anyhow::Result<User> {
    self.query_user(id).await
}
```

推荐应用配置：

```yaml
redis:
  url: ${APP_REDIS_URL}
  namespace: order-service
  profile: RustV2

cache:
  mode: two_level
  redis_ref: default
  cache_ttl_secs: 300
  null_ttl_secs: 30
  invalidation:
    enabled: false
    redis_url: ""
```

分工说明：

| 事项 | 位置 |
| --- | --- |
| `scene`、`key`、`refresh_ms`、`expire_ms` | 宏属性 |
| Redis 连接和 L2 初始化 | 受管 `"cache"` 组件，或手工持有 `CacheRuntimeGuard` |
| 是否启用缓存 | `cache.mode` |
| 写后失效目标 | `#[cache_invalidate]` 属性 |

使用建议：

| 场景 | 写法 |
| --- | --- |
| 普通读缓存的写后失效 | 写 DB 成功后使用 `#[cache_invalidate]` 或显式失效。 |
| 只想进程内缓存 | 使用 `cacheable::local_cache`，不要把两级缓存宏当作纯 L1。 |
| 需要区分不同租户 | 把租户 ID 写入 `key = "tenant:{tenant_id}:user:{id}"`。 |
| 查询在事务内执行 | 由业务决定是否允许读取缓存;需要强一致时拆出不带缓存的新方法。 |

## 主要边界

- 两个宏都只支持 `async fn`；`#[cached]` 的 loader 可能进入后台刷新，因此接收者必须拥有
  `'static` 生命周期，方法应使用 `self: Arc<Self>`。
- 同一 `scene` 的值类型、`refresh_ms` 和 `expire_ms` 必须一致；受管缓存组件会在 Start 阶段审计。
- `#[cache_invalidate]` 只在原函数成功后失效，但失效错误只记录告警，不改写业务返回值。因此它提供
  普通缓存的最终一致性，不是资金、库存或权限写入的强一致性边界。
- 两级缓存宏要求运行时已经安装 backend；受管应用应声明 `"cache"`，手工装配必须持有并关闭
  `CacheRuntimeGuard`。
