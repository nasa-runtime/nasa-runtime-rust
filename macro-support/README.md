# macro-support

`macro-support` 是 nasa-runtime-rust 过程宏共用的运行时路径解析器，不是业务 API。

它解决的问题是：业务项目可能只依赖门面 `nasa`，也可能处于旧 crate 直连迁移期，还可能把 `nasa` 在 Cargo 中重命名。宏展开时不能硬编码 `::nasa::...` 或 `::hystrix`，必须按调用方实际依赖解析路径。

主要规则：

- 旧直接依赖优先，例如直接依赖 `natx` 时展开为 `::natx`。
- 没有旧依赖时回退门面，例如展开为 `::nasa::tx` 或 Cargo 重命名后的 `::company_nasa::tx`。
- `naweb-macro` 保留旧布局兼容：直接依赖宏 crate 时仍可走裸 `::axum` / `::linkme` / `::tracing`。
- 两类依赖都缺时返回错误文案，宏侧转成 `compile_error!`，提示"依赖 `nasa`(features 含对应模块)或直接依赖运行时 crate"——不会 panic，也不会产生难定位的路径错误。

路径解析同时覆盖 handler 包装、直接依赖、门面回退、Cargo rename 和缺依赖报错，并兼容 library
与 Cargo 独立编译目标。发布归档仅包含路径解析实现、使用说明和许可文件。

宏 crate 使用示例：

```rust
let root = macro_support::runtime_root("tx", "natx")?;
```

业务项目不应直接调用本 crate。

## YML 配置与使用

`macro-support` 没有 yml 配置，也没有运行期初始化。它只在过程宏展开期间读取 Cargo 依赖元数据，用来决定生成代码里的 crate 路径。

业务侧需要配置的是 Cargo feature，不是 yml：

```toml
nasa = { version = "1.0.0", features = ["tx", "mapper", "cache"] }
```

规则说明：

| 场景 | 结果 |
| --- | --- |
| 业务直接依赖运行时 crate | 宏优先展开到直接依赖路径。 |
| 业务只依赖 `nasa` 门面 | 宏展开到 `nasa::<module>`。 |
| 业务把 `nasa` 重命名 | 宏展开到重命名后的门面路径。 |

排错建议：

| 现象 | 处理方式 |
| --- | --- |
| 宏报找不到运行时 crate | 在应用 Cargo feature 中开启对应门面模块,例如 `tx`、`mapper`、`cache`。 |
| 门面 crate 被重命名后宏路径异常 | 确认应用只保留一个门面依赖名称,避免同时出现多个别名。 |
| 只依赖宏 crate 编译失败 | 补上 `nasa` 门面或直接依赖对应运行时 crate。 |

不要在应用 yml 里为本 crate 增加配置项；它不会读取，也没有可调运行时行为。

## 主要边界

- 本 crate 只供仓库内过程宏实现使用，业务代码不应把路径解析器当作运行时 API。
- 路径解析基于调用方 Cargo 依赖名，不读取应用 yml，也不探测运行时模块。
- 新宏必须同时覆盖直接依赖、门面依赖、门面重命名和缺依赖报错四条路径。
- 错误应在宏调用位置形成清晰的编译错误，不能 panic，也不能硬编码门面 crate 名。
