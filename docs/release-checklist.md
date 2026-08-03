# 发布检查清单

公开打 tag 或发布包前，按本清单逐项确认。

## 仓库检查

- [ ] `LICENSE`、`LICENSE-MIT`、`LICENSE-APACHE` 已存在，并与 `Cargo.toml` 的 license 一致。
- [ ] `NOTICE` 已存在，且每个 `.crate` 归档都包含两份许可证正文与 `NOTICE`。
- [ ] `README.md` 已列出所有工作区组件和门面 feature。
- [ ] 每个组件 crate 都有自己的 `README.md`。
- [ ] 每个组件 README 都包含配置、初始化、示例和边界说明。
- [ ] `CHANGELOG.md` 已写入本次发布条目。
- [ ] `SECURITY.md` 已覆盖当前安全敏感面。
- [ ] `CONTRIBUTING.md` 已包含当前检查命令和维护约束规则。
- [ ] CI 能在干净 Linux runner 上通过。
- [ ] 远端默认分支已建立并推送，分支保护规则已配置。
- [ ] GitHub private vulnerability reporting 已开启，`SECURITY.md` 中的私密报告链接可用。

## 构建检查

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo test --workspace`
- [ ] `cargo clippy --workspace --all-targets -- -D warnings`
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- [ ] `RUSTDOCFLAGS="-D missing_docs -D warnings" cargo doc --workspace --no-deps`
- [ ] `cargo check -p nasa --features full --all-targets`
- [ ] `cargo deny check`
- [ ] 本批合同测试全部位于仓库根 `tests/`；组件 crate 内无测试目录/测试 target，`.crate` 归档无测试文件、
      测试代码或测试性文档/注释。
- [ ] 第一批 1A 的逐包 `cargo package --offline` 通过；1B 的 `naml` 在 `nabase = 1.0.0` 可从索引解析后再验证。

## 文档检查

- [ ] 禁用词扫描在 `target/` 之外无命中。
- [ ] 已删除组件残留扫描在 `target/` 之外无命中。
- [ ] README 覆盖扫描显示根 README 和所有组件 README 都是 `cfg=ok code=ok usage=ok`。
- [ ] 所有组件 README、rustdoc、源码注释和 manifest 注释不含测试性内容；`namapper` 的动态 SQL
      `test` 仅作为公开协议属性出现。
- [ ] yml 示例不包含真实私有地址、密码或 token。

## 后端检查

- [ ] SQL 或缓存行为变化时，Mapper 真实 MySQL + Redis 验收通过。
- [ ] 命令、pipeline、锁、Stream 或缓存行为变化时，Redis 真实后端验收通过。
- [ ] 认证、帧处理、端点、通知或背压行为变化时，WebSocket 真实后端验收通过。
- [ ] 集群锁、重复抑制或 cron 行为变化时，调度真实后端验收通过。
- [ ] 注册映射、watch 或负载均衡行为变化时，服务发现真实后端验收通过。
- [ ] 所有真实后端测试都清理 Redis key 和数据库测试行。

## 发布检查

- [ ] 工作区包版本与发布计划一致。
- [ ] 所有公开 crate 的 `description` 准确。
- [ ] 所有 crate 均解析为 `1.0.0`，并继承统一的 repository、homepage、Rust 1.94 MSRV、keywords 和 categories。
- [ ] 初次发布前实时查询 crates.io；不存在非本项目 owner 占用的同名 crate。
- [ ] `nasa` 门面 feature 包含所有计划公开的可选组件。
- [ ] 公开发布包不依赖未发布的本地 sibling path。
- [ ] 当前拓扑阶段每个待发布 crate 的 `cargo publish --dry-run -p <crate>` 通过。
- [ ] 发布顺序符合 `docs/publishing.md` 的六阶段依赖顺序，且上一阶段已能从 crates.io 解析。

## 发布后检查

- [ ] tag 已推送。
- [ ] 发布说明链接到 `CHANGELOG.md`。
- [ ] 门面 crate 和主要组件 crate 的文档能正常渲染。
- [ ] 干净下游示例只依赖已发布门面包即可编译。
