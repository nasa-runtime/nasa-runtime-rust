# 发布指南

`nasa-runtime-rust` 是多 crate 工作区。公开使用者应优先依赖 `nasa` 门面 crate；内部 crate 也需要可发布，因为门面会按 feature 依赖这些实现 crate。

## 许可证

所有 crate 使用：

```toml
license = "MIT OR Apache-2.0"
```

仓库包含：

- `LICENSE`：双许可证入口说明；
- `LICENSE-MIT`：MIT 许可证正文；
- `LICENSE-APACHE`：Apache-2.0 许可证正文。
- `NOTICE`：项目名称的独立性与无官方背书声明。

每个 crate 目录都以符号链接携带 `LICENSE-MIT`、`LICENSE-APACHE` 和 `NOTICE`。发布前必须通过
`cargo package --workspace --list` 确认这些文件实际进入每一个 `.crate` 归档；只在仓库根保留许可证
不能满足源码归档的再分发要求。

## 版本规则

- 全部工作区 crate 的初始公开版本统一为 `1.0.0`，由根 `[workspace.package]` 单点管理。
- 同一个 minor 版本线内，patch 发布应保持兼容。
- 删除公开 API、修改 yml 键、重命名 feature，都必须在 `CHANGELOG.md` 中写兼容性说明。
- 宏行为变化必须说明生成代码影响和涉及的 feature。
- 名称含 `experimental` 或在文档中明确标记实验性的 feature 不进入 `full`，其 API 在晋升前不纳入
  稳定兼容承诺；任何破坏性调整仍必须写入 `CHANGELOG.md` 和迁移说明。稳定 feature 不适用该豁免。

## 初次发布前的名称检查

初次发布不能假设包名仍然可用。紧邻正式发布窗口，对工作区全部包逐一查询 crates.io：

```bash
cargo metadata --no-deps --format-version 1 \
  | jq -r '.packages[].name' \
  | sort -u \
  | while IFS= read -r crate_name; do
      code=$(curl -sS -L -o /dev/null -w '%{http_code}' \
        -H 'User-Agent: nasa-runtime-rust-release-check/1.0' \
        "https://crates.io/api/v1/crates/${crate_name}")
      printf '%s %s\n' "$code" "$crate_name"
    done
```

尚未发布的包应返回 `404`。若返回 `200`，必须继续查询
`https://crates.io/api/v1/crates/<crate>/owners`：只有 owner 明确属于本项目发布账号时才能沿用；
否则先修改 crate 名、门面依赖、README、发布拓扑和下游示例，再重新执行全量检查。

前五批已经由本项目账号完成发布。其余批次的名称状态会随外部注册表变化，历史结果不能替代
正式上传前的最后一次复查。

## 发布批次与顺序

首次发布时，依赖包尚不存在于 crates.io，后续阶段的 `cargo publish --dry-run` 会在解析 sibling
版本依赖时失败。因此必须按下面的拓扑阶段发布；每一阶段全部进入 crates.io 索引后，才能 dry-run
下一阶段。

1. 第一批 1A，无内部 workspace 依赖的公开合同：`macro-support`、`nabase`、`nabudget`、`nadisc`、
   `naauthz`、`naidempotency`、`nainbox-core`、`nametrics-core`、`naopenapi`、`naoutbox-core`、
   `natelemetry`。
2. 第一批 1B，配置合同：`naml`。它依赖 `nabase = 1.0.0`，必须等 1A 的 `nabase` 可被
   crates.io 稀疏索引解析后再发布。
3. 第二批，其余叶子：`nadate`、`nagrpc`、`naimg`、`namigrate`、`nanum`、`napart`、`nasecret`、
   `naws-proto-derive`、`ncrypto`、`rest-client-macro`。
4. 第三批，宏、adapter 和协议层：`async-macro`、`hystrix-macro`、`naaudit`、`nacache-macro`、
   `nadis-derive`、`nafana-macro`、`nafka-macro`、`nalog`、`namapper-macro`、`nanacos`、
   `naobject`、`napp-macro`、`nasecret-http`、`nasecret-vault`、`natx-macro`、`nauth-oauth`、
   `naweb-macro`、`naws-proto`。
5. 第四批，核心运行时：`cacheable`、`config-boot`、`hystrix`、`nadis`、`nafana`、`nafka`、
   `natx`、`naweb`、`rest-discovery`。
6. 第五批，持久化 adapter 与组合运行时：`naidempotency-mysql`、`naidempotency-redis`、
   `nainbox-mysql`、`namapper`、`naoutbox-mysql`、`nasched`、`naws`、`rest-discovery-nacos`。
7. 第六批，应用编排层：`naaudit-mysql`、`napp`。
8. 第七批，唯一业务门面：`nasa`。

如果某个 crate 依赖另一个本地 crate，必须先发布被依赖项。

前六批已完成实际发布；后续批次状态不能因本地工作区可编译就提前标记为可发布。

一旦内部 crate 的目标版本已进入 crates.io，所有组件 manifest 必须移除指向它的 `path`，只保留
registry `version`。`path + version` 仅允许用于尚未发布的后续拓扑边；不参与发布的根级质量工程
可按验证目的显式选择工作区路径。发布工作流会扫描全工作区，拒绝任何组件继续通过 `path` 引用
已发布 crate，并拒绝当前待发布 crate 含任意本地依赖。

## 发布演练

对当前阶段的每个 crate 先运行归档验证或 dry-run：

```bash
cargo publish --dry-run -p macro-support
cargo publish -p macro-support
```

GitHub Actions 的 `.github/workflows/publish-crates.yml` 通过手工输入选择发布批次。必须从受保护的
release 分支选择目标批次并输入工作流要求的确认文本。crates.io token 只授予当前批次需要的 scope，
并将当前批次的精确 crate 名加入允许范围。

发布后等待 crates.io API 和稀疏索引均能解析该版本，再继续同阶段下一个包。阶段完成后，以干净
临时目录执行下一阶段 dry-run；不得使用 `[patch]` 或本地 path 成功冒充注册表依赖可解析。

## 公开依赖检查

发布前确认每个 crate 都具备：

- `description`；
- `license`；
- `edition`；
- `repository`、`homepage` 和统一 MSRV（Rust 1.94）；
- `.crate` 归档中的两份许可证正文及 `NOTICE`；
- `.crate` 归档只包含产品源码、公开文档和再分发所需文件，不携带内部验证资产、证书或脚本；
- 不依赖只适用于私有本地环境的 path-only 依赖；
- README 与该 crate 的公开 API 一致。

## 门面 feature 检查

门面必须能在全部公开 feature 下编译：

```bash
cargo check -p nasa --features full --all-targets
```

也要至少编译一个下游服务常用的最小 feature 集合：

```bash
cargo check -p nasa --no-default-features
```

## 发布说明

发布说明应包含：

- 变更的 crate；
- 新增或重命名的 feature；
- yml 键变化；
- 迁移说明；
- 后端兼容性说明；
- 如果提到吞吐或延迟，需要附带性能证据。
