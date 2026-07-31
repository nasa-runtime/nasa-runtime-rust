# 贡献指南

`nasa-runtime-rust` 是一个多 crate 工作区。业务应用优先只依赖 `nasa` 门面 crate，再按需要开启对应 feature。内部 crate 仍保留给实现、宏展开和渐进迁移使用，但公开示例应优先展示门面用法。

## 开发检查

提交变更前，根据变更风险运行对应检查。即使只改文档，也需要至少完成格式检查和禁用词扫描。

```bash
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy --workspace --all-targets --all-features -- -D warnings
RUSTDOCFLAGS="-D missing_docs -D warnings" cargo doc --workspace --no-deps
```

如果本机 `PATH` 中已有 `cargo`，可以直接使用 `cargo`。

## 文档规则

- 每个组件 crate 必须有自己的 `README.md`。
- 每个组件 README 必须说明：用途、feature 或依赖接入方式、yml 配置、初始化方式、至少一个正常使用示例，以及主要边界条件。
- 根 README 只作为索引和门面使用指南；组件细节应写在组件自己的 README 中。
- 公开 `struct`、`trait`、`enum`、`fn` 和重要参数需要业务上下文注释，不能写泛泛的占位说明。
- `namapper/README.md` 保持业务使用指南，不写测试目录或测试属性示例。
- 不要把真实密钥、本地密码、私有主机名或内部网络地址写进已提交文档。

## 代码规则

- 改动应收敛在目标组件内，避免顺手重构无关模块。
- 优先沿用现有门面和 feature 布局，不新增不必要的跨 crate 依赖模式。
- 修改宏行为时，若生成代码对使用者可见，需要同步更新宏 crate README 和运行时 crate README。
- 修改缓存和事务行为时，必须明确说明一致性语义。
- 修改 Redis、MySQL、WebSocket、服务发现或调度行为时，相关文档应说明失败处理、超时、队列或资源上限。
- 修改公开 API 时，应先补测试或同步更新现有测试，再更新文档。

## 测试要求

测试、fixture、探针和实机场景只允许位于 gitignored 的根 `tests/`，或仓库外
`application-demo` / `application-demo2`；不得把测试属性、测试 target、测试专用接口或测试脚本
提交到产品 Git。贡献者需要在变更说明中写清验证场景，由维护者在本地验收资产中复现并记录结果。

| 变更类型 | 最小检查 |
| --- | --- |
| 只改 README 或注释 | 格式检查、禁用词扫描、README 覆盖扫描 |
| 单个基础工具 crate | 本地根 `tests/` 对应用例 + 工作区 clippy |
| 宏展开行为 | 本地根 `tests/` 的 UI/下游编译覆盖 |
| Redis 或缓存行为 | 本地根 `tests/` 覆盖 + 涉及协议、TTL 或真实缓存语义时跑真实后端验收 |
| MySQL、mapper 或事务行为 | 本地根 `tests/` 覆盖 + 涉及 SQL 或事务语义时跑真实后端验收 |
| 门面 feature 编排 | `cargo check -p nasa --features full --all-targets` |

## 维护重点

- feature 单独开启和组合开启都能编译。
- 生成代码使用正确的门面路径，特别是依赖改名后的路径。
- 事务内缓存行为必须显式，不能让使用者误解共享 L2 的可见性。
- Redis 和 WebSocket 路径必须有队列、超时、连接数或并发上限，并有可见错误处理。
- 配置解析对非法值应尽早失败。
- 公开文档必须与真实行为和默认值一致。
