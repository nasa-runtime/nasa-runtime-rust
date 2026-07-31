# nalog

`nalog` 是基于 `tracing` 的日志组件，提供兼容旧 logback 风格的 formatter、运行期级别热切、按天和按大小滚动、保留清理、独立 error.log。

业务项目通过门面开启 `log`：

```toml
[dependencies]
nasa = { version = "1", features = ["log"] }
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
| `pattern` | 内置 pattern | logback 风格输出 pattern；也兼容 `log_pattern`、`logPattern`、`LOG_PATTERN`。 |

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

热刷新日志级别时只需要调用 `nasa::log::set_level(&new_cfg.log.level)`；文件目录和滚动策略属于
appender 生命周期配置，通常随进程重启生效。

## 主要边界

- 初始化 owner 只能有一个；重复安装 subscriber 或文件 appender 会返回明确错误。
- 运行期只热切 level，目录、pattern 和滚动策略需要重启后生效。
- 日志字段不得包含 secret、token、连接串、请求正文或未脱敏身份信息。
- 文件 guard 必须由应用生命周期持有到停机 flush 完成。
