# nafana-macro

`nafana-macro` 实现 `#[grafana]`。它在编译期校验隔离、超时、TPS 和降级参数，把异步 handler 包装进
`nafana` 命令运行时，同时保留原函数签名。

业务只从门面使用：

```toml
[dependencies]
nasa = { version = "1", features = ["grafana", "web"] }
```

```rust
use nasa::grafana::grafana;

#[grafana(
    name = "order-detail",
    max_concurrent = 32,
    timeout_ms = 800,
    tps = 1
)]
#[nasa::web::get_mapping("/orders/{id}")]
async fn detail() -> impl axum::response::IntoResponse {
    "ok"
}
```

与 mapping 组合时，`#[grafana]` 必须写在 mapping 属性上方，宏才能取得稳定路由名。

## 参数

| 参数 | 说明 |
| --- | --- |
| `name` | 指标和面板中的稳定命令名；缺省优先取下方 mapping path。 |
| `max_concurrent` | 信号量容量；省略或 `0` 表示不限。 |
| `timeout_ms` | 执行超时；省略或 `0` 表示不设置。 |
| `tps` | TPS 权重；省略表示不标记，显式 `0` 表示权重为零。 |
| `reject_response` / `timeout_response` | 编译期校验的静态 JSON 降级响应。 |
| `reject_fn` / `timeout_fn` | 零参异步降级函数；与同路径静态响应互斥。 |

## YML 配置

宏本身不读取 yml。编译期固定策略写在属性上；需要按环境变化的规则使用 `nafana` 配置驱动隔离入口。

## 主要边界

- 只支持 `async fn` handler。
- 静态降级正文必须是合法 JSON。
- 同一路径的函数降级和静态降级不能同时声明。
- 宏只做包装和路径解析；指标目录、执行状态和导出由 `nafana` 负责。
