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

凭据、证书、临时输出和内部路径不得进入归档。

## 依赖边界

公开 crate 的同仓依赖只使用 registry 版本约束，不携带本地 `path`。归档内规范化后的 manifest 必须
保持相同约束，不能用工作区覆盖掩盖缺失依赖。

`natx` 的事务上下文以及 `naoutbox-mysql` 的提交唤醒都绑定到具体 package 实例。最终可执行制品若同时
装入 registry 与本地路径副本，两份实例会各自持有互不可见的进程状态，表现为事务归属、消息收集或
提交唤醒静默失效。每个制品在锁定依赖后必须分别执行 `cargo tree -i natx`、`cargo tree -i nafka`、
`cargo tree -i naoutbox-core` 和 `cargo tree -i naoutbox-mysql`，确认每个包只有一个 package ID 和一个来源；发现分裂时应先统一依赖坐标，
再重新生成锁文件。包内运行时检查不能替代这项门禁，因为彼此隔离的包实例无法枚举或读取对方的静态状态。
`nainbox-mysql` 也必须执行相同检查，确保部署建表入口与 Saga 运行时使用同一份 schema 合同。

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
