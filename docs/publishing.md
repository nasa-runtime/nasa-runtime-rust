# 公开归档与依赖拓扑

`nasa-runtime-rust` 是多 crate 工作区。业务使用者优先依赖 `nasa` 门面；门面按 feature 引用内部实现
crate，因此公开归档必须遵循依赖拓扑。

## 许可证

全部 crate 使用双许可证：

```toml
license = "MIT OR Apache-2.0"
```

每个归档都包含 `LICENSE-MIT`、`LICENSE-APACHE` 和 `NOTICE`。只有仓库根存在许可证文件不足以满足
源码再分发要求。

## 归档内容

公开 `.crate` 只包含：

- 产品源码与 manifest；
- 当前 README 与 rustdoc 所需静态资源；
- 许可证与 `NOTICE`；
- 运行时确实需要的迁移 SQL、面板或协议资源。

根级质量工程、fixture、故障注入工具、凭据、证书、临时输出和内部路径不得进入归档。

## 依赖阶段

公开顺序按依赖从底向上推进：

1. 无内部依赖的基础类型、宏支持和纯工具 crate；
2. 核心合同与独立宏 crate；
3. 数据库、缓存、消息和安全适配器；
4. 应用组件桥与运行时；
5. `nasa` 门面。

同一阶段只有在前置 crate 已能从 registry 正常解析后才能继续。已公开的内部依赖只保留 registry
坐标；尚未公开的同仓依赖可在开发期使用 `path`，进入归档前必须移除，避免本地路径掩盖缺失依赖。

## 操作命令

查看归档内容：

```bash
cargo package -p <crate> --locked --offline --list
```

生成归档但不上传：

```bash
cargo publish --dry-run -p <crate> --locked
```

上传后等待 registry 索引能够解析当前 crate，再推进依赖它的下游 crate。禁止用工作区 `[patch]` 掩盖
registry 尚不可用的事实。

## 门面要求

- `nasa` feature 与组件 README、根索引和实际依赖一致；
- 稳定能力的组合由 `full` 显式列出；实验能力不进入 `full`；
- 宏生成代码通过门面路径引用运行时，不要求业务直接依赖宏实现 crate；
- yml 键、公开 API 或 feature 行为变化时同步提供迁移说明；
- 包名占用、owner 权限和 registry 状态在实际上传窗口确认，不依赖历史结论。

交付前条件见 [交付就绪清单](release-checklist.md)。
