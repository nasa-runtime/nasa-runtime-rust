# nalog

`nalog` 是基于 `tracing` 的日志组件，提供可配置 formatter、运行期级别热切、按天和按大小滚动、
保留清理与独立 error.log。

业务项目通过门面开启 `log`：

```toml
[dependencies]
nasa = { version = "2.0.0", features = ["log"] }
```

## 控制台日志

```rust
fn main() {
    nasa::log::init();
    tracing::info!("service started");
}
```

带默认过滤级别：

```rust
nasa::log::init_with_default("info,sqlx=warn");
```

## 运行期调整级别

```rust
nasa::log::set_level("debug,nacos=info");
```

该入口适合配置中心热刷新后调整日志级别。

## 文件日志

简单启用：

```rust
let _guard = nasa::log::enable_file_logging(Some("logs"));
```

生产建议使用强类型配置，并持有 `LogGuard` 到进程退出。

```rust
let cfg = nasa::log::FileLogConfig::new("logs")
    .with_max_file_size_mb(Some(100))
    .with_max_history_days(Some(30))
    .with_total_size_cap_mb(Some(10_240))
    .with_color(false);

let guard = nasa::log::try_enable_file_logging_with(&cfg)?;
```

## 停用文件日志

```rust
nasa::log::disable_file_logging();
```

## 使用建议

- 应用启动早期先 `init()` 打控制台，配置就绪后再启用文件日志。
- 文件日志 guard 必须持有，否则后台 appender 会被 drop。
- `FileLogConfig` 的保留策略用于防止日志目录无限增长。

## YML 配置与使用

推荐把日志配置放在 `log:` 根节点，并反序列化为 `nasa::log::LogConfig`。配置就绪后调用
`resolve` 转成 `ResolvedLogConfig`，再启用文件日志。

完整示例：

```yaml
log:
  level: info,my_app=debug,sqlx=warn
  path: logs/order-service
  max_file_size: 500MB
  total_size_cap: 30GB
  max_history_days: 30
  clean_history_on_start: true
  split_error_file: true
  color: false
  pattern: "%d{yyyy-MM-dd HH:mm:ss.SSS} %-5level [%thread] %logger - %msg%n"
```

字段说明：

| 键 | 默认值 | 说明 |
| --- | --- | --- |
| `level` | `info` | `tracing_subscriber::EnvFilter` 表达式，支持模块级覆盖。 |
| `path` | `null` | 非空时写 `info.log` 和可选 `error.log`；为空时按路径策略只打控制台或使用默认目录。 |
| `max_file_size` | `500MB` | 单文件滚动上限；也兼容 `maxFileSize`。 |
| `max_file_size_mb` | `null` | 兼容旧字段；与 `max_file_size` 同时存在时后者优先。 |
| `total_size_cap` | `30GB` | 归档总量上限；也兼容 `totalSizeCap`。 |
| `total_size_cap_mb` | `null` | 兼容旧字段；与 `total_size_cap` 同时存在时后者优先。 |
| `max_history_days` | `30` | 归档保留天数；也兼容 `maxHistory`。 |
| `clean_history_on_start` | `true` | 启动时是否清理过期归档；也兼容 `cleanHistoryOnStart`。 |
| `split_error_file` | `true` | 是否单独写 `error.log`。 |
| `color` | `false` | 文件日志是否输出 ANSI 颜色。 |
| `pattern` | 内置 pattern | 输出 pattern；也接受 `log_pattern`、`logPattern`、`LOG_PATTERN`。 |

启动代码：

```rust
#[derive(serde::Deserialize)]
struct AppConfig {
    log: nasa::log::LogConfig,
}

nasa::log::init_with_default(&cfg.log.level);
let ctx = nasa::log::LogContext {
    app_name: Some("order-service".to_string()),
    ..Default::default()
};
let resolved = cfg.log.resolve(&ctx)?;
let _guard = if let Some(file) = &resolved.file {
    Some(nasa::log::try_enable_file_logging_with(file)?)
} else {
    None
};
```

独立调用 `nasa::log::set_level(&new_cfg.log.level)` 只调整过滤级别，不会替换文件 appender。
需要同时应用目录、pattern 或滚动策略时，由唯一 owner 使用 `LogManager` 准备并安装新配置；
Application 的 `"log"` 组件已负责这一流程。

## 主要边界

Application 的 `"log"` 组件持有文件 guard，业务停机任务和资源清理先于日志收口。业务自有日志适配器
可由唯一 owner 管理，但不要再用 `register_graceful_shutdown` 重复关闭受管 appender。
停机失败与最终摘要使用运行时独立的同步诊断通道，不依赖日志组件继续存活。

- 初始化 owner 只能有一个；重复安装 subscriber 或文件 appender 会返回明确错误。
- 受管日志支持级别、目录、pattern 与滚动配置的候选准备和安装；失败时保留已生效输出。
- 日志字段不得包含 secret、token、连接串、请求正文或未脱敏身份信息。
- 文件 guard 必须由应用生命周期持有到停机 flush 完成。

## Application 接入

门面开启 `application,log` 并声明 `"log"`；本地文件监听或 Nacos 更新驱动配置候选。
组件先在发布锁外准备日志文件，再在 `publication_gate` 内同步安装日志并发布实际 reload 状态。

```text
候选 log 配置 → prepare：打开资源，保留当前输出 → install：切换 writer 与过滤器
                                                          ↓
                                     发布 ConfigView → 锁外回收旧 guard
```

prepare 不提前滚动或清理当前文件；install 不执行外部 I/O。旧 guard 交给单线程、有界回收器，
停机等待回收退出。安装前取消可以丢弃候选；安装开始后必须完成同次配置视图与状态发布。

配置与完整生命周期边界见 [受管能力合同](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/managed-capabilities.md)。
