# cacheable

`cacheable` 提供两级缓存运行时、强类型 L1、本地单飞加载、L2 Redis 适配和跨实例失效广播。
业务项目通过 `nasa` 门面开启 `cache` feature；使用应用运行时时可声明 `"cache"` 组件，由容器拥有
安装、代际切换和停机责任。

```toml
[dependencies]
nasa = { version = "1", features = ["application", "cache", "redis"] }
```

## 受管初始化

`"cache"` 组件在 Start 阶段审计全部 scene，建立 L2，安装 `CacheRuntime`，并在停机时排空失效广播。
`redis_ref` 复用已经受管的 Redis 客户端；`redis_url` 让缓存组件自建 Redis Cluster 连接。
两级模式还会登记 `cache:l2` readiness 并周期执行只读 `PING`：运行期故障降级而不摘流，启动期
连接失败仍拒绝进入 Ready。

```rust
#[nasa::application("redis", "cache")]
async fn main(_app: nasa::Application) -> anyhow::Result<()> {
    Ok(())
}
```

`cache` 不强制应用进入 Service 模式，因此批处理也能在 Start 建立缓存并在退出前完成清理。

## 手工初始化

不使用应用运行时时，调用方必须显式拥有连接、运行时 guard 和停机过程：

```rust
let redis = cacheable::connect_cluster(&cfg.redis_url).await?;
let layer = std::sync::Arc::new(cacheable::cache::CacheLayer::new(redis, 300, 30));
let guard = cacheable::CacheRuntimeGuard::start(layer, None).await?;

// 进程停机时执行：
guard.shutdown().await;
```

不要在同一进程中让多个 owner 交叉调用旧式 `init` 和受管 guard；代际 owner 用来保证旧 guard
不能撤销后来安装的新运行时。

只读诊断通过 `cacheable::cache_handle()` 获取。`snapshot()` 只暴露 generation 与能力是否安装，
`health_check()` 固定当前代后端执行探针；句柄不拥有连接、广播任务或停机权限。

## 宏读缓存

```rust
use nasa::cache::cached;
use std::sync::Arc;

#[cached(
    scene = "kline",
    key = "kline:{symbol}:{period}",
    refresh_ms = 1_000,
    expire_ms = 3_000
)]
async fn find(
    self: Arc<Self>,
    symbol: String,
    period: u8,
) -> anyhow::Result<Vec<Row>> {
    self.query_db(symbol, period).await
}
```

loader 可能进入后台刷新任务，所以方法接收者不能借用 `&self` / `&mut self`，应使用
`self: Arc<Self>` 或自由函数。

## 写后失效

```rust
use nasa::cache::cache_invalidate;

#[cache_invalidate(
    scene = "kline",
    key = "kline:{symbol}:{period}",
    value = "Vec<Row>"
)]
async fn save(
    &self,
    symbol: String,
    period: u8,
    rows: Vec<Row>,
) -> anyhow::Result<()> {
    self.save_db(symbol, period, rows).await
}
```

`value` 必须与同一 scene 的缓存值类型一致。组件启动时会审计同名 scene 的值类型与 TTL 合同，
不把不一致延迟到首个请求。

## 两种缓存模型

- `CacheLayer`：cache-aside，两级命中、空结果短缓存和 miss 单飞，适合 TTL 驱动的热点读。
- `GroupedCache`：Redis Hash 分组缓存，适合写后主动失效和关联失效；字段 TTL 依赖 Redis 7.4+
  的 `HPEXPIRE`。
- `local_cache`：纯进程内加载、刷新、过期和移除工具，不提供跨副本一致性。

## YML 配置与使用

声明 `"cache"` 时，`napp` 严格读取下面的 `cache:` 投影；未知字段会在建立连接前被拒绝。

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
    enabled: true
    redis_url: ${APP_REDIS_URL}
```

| 键 | 默认值 | 说明 |
| --- | --- | --- |
| `cache.mode` | `disabled` | `disabled` 或 `two_level`。 |
| `cache.redis_ref` | 无 | 复用 `"redis"` 组件中的命名实例；与 `redis_url` 互斥。 |
| `cache.redis_url` | 空 | 自建 L2 Cluster 连接；与 `redis_ref` 互斥。 |
| `cache.cache_ttl_secs` | `300` | 普通值基础 TTL。 |
| `cache.null_ttl_secs` | `30` | 空结果哨兵 TTL。 |
| `cache.invalidation.enabled` | `false` | 是否启动跨实例 L1 失效广播。 |
| `cache.invalidation.redis_url` | 空 | 广播连接；启用广播时必填。 |

方法级 `refresh_ms` 和 `expire_ms` 仍写在宏属性上，不由组件 yml 猜测。

## 主要边界

- `mode: disabled` 不建连、不安装 backend，但仍执行 scene 静态合同审计。
- `two_level` 必须在 `redis_ref` 与 `redis_url` 中选择一个；同时配置也会被拒绝。
- `redis_ref` 要求同时声明 `"redis"`，容器固定按 `redis -> cache` 顺序启动。
- L2 运行期不可达时 readiness 为 Degraded；缓存 miss 仍可回源，所以默认不把该故障升级为摘流。
- 广播关闭时，失效只保证当前进程可见；广播不是持久日志，断线期间不提供历史补偿。
- `BoundedInvalidatePublisher::channel` 的非 fallible 容量参数收敛到 Tokio 有界队列可表达范围，
  不因零值或极端值在构造时 panic。
- 缓存不是业务事实源；资金、库存和权限判断不能把缓存命中当作最终一致性证明。
