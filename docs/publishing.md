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

## 文档归档门禁

代码、单元测试和 `cargo package` 能执行都不等于公开文档完整。每个待发布 crate 必须从生成的
`.crate` 文件直接核对，而不是只读工作树：

1. 解包后打开 README，确认首屏准确说明核心价值、适用场景和明确不解决的问题。
2. 对照独立架构章节、crate rustdoc、manifest description、keywords/categories，确认定位一致。
3. 检查示例只依赖归档外真实可访问的公开链接；crate README 不引用归档中不存在的 `../docs`。
4. 对照实际 feature 和公开 API 检查配置、初始化、失败语义、观测、停机与恢复边界。
5. 核对规范化 manifest、许可证、迁移 SQL和最终文件清单，再给出文档验收结论。

Saga 发布至少覆盖 `nasaga-core`、`nasaga-mysql`、`nasaga-runtime`、`nasaga-macro`、`napp`、`nasa`，
并同步检查根 README、Saga 生产指南、迁移指南、运维指南、告警规则以及 Inbox/Outbox 交叉合同。

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

Saga 依赖顺序为：先发布并回读 `nasaga-core`，再发布 `nasaga-mysql` 与 `nasaga-macro`，随后发布
`nasaga-runtime`，最后才是依赖它的 `napp` 与 `nasa`。每一步都要等 registry 能从一个不带本地 patch
的隔离解析图取得前置版本。

默认发布流程是完成提交、推送目标分支、远端 CI 全绿、核对远端 SHA、取得明确发布授权、上传 registry、
回读 registry 元数据与 README。dry-run 或本地归档通过不构成上传授权；公开版本不可原地替换，发现归档
遗漏时必须停止并使用新的补丁版本承载后续内容。

## 门面要求

- `nasa` feature 与组件 README、根索引和实际依赖一致；
- 稳定能力的组合由 `full` 显式列出；实验能力不进入 `full`；
- 宏生成代码通过门面路径引用运行时，不要求业务直接依赖宏实现 crate；
- yml 键、公开 API 或 feature 行为变化时同步提供迁移说明；
- 包名占用、owner 权限和 registry 状态在实际上传窗口确认，不依赖历史结论。

交付前条件见 [交付就绪清单](release-checklist.md)。
