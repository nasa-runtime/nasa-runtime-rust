# nasa-runtime-rust

[中文](README.md) | [English](README.en.md)

**Managed application lifecycles and reliable business execution for Rust server applications.**

`nasa-runtime-rust` brings configuration validation, resource preparation, business initialization,
traffic admission and ordered shutdown into one application lifecycle. Transactions, Inbox/Outbox,
Saga and ordered message processing support recoverable business workflows. Applications select
capabilities through Cargo features and use their databases and message systems to hold durable facts.
Each component keeps explicit resource, transaction and failure boundaries.

Configuration assembly supports nested defaults, naturally ordered filename imports and bounded,
explainable candidates. Source observations remain separate from component application state; rejected
candidates preserve the current view. See [Strict configuration assembly](#strict-configuration-assembly).

Applications use the **`nasa` facade** as their entry point. The workspace contains implementation
and macro crates; applications normally select facade features rather than assemble those crates
individually. The framework builds on Tokio and existing infrastructure. It does not provide a new
async executor, database or message broker.

| Start here | English | 中文 |
| --- | --- | --- |
| Run a complete HTTP service | [Quickstart](docs/quickstart.en.md) | [快速开始](docs/quickstart.md#最小可运行服务) |
| Understand ownership and consistency | [Architecture](docs/architecture.en.md) | [架构说明](docs/architecture.md) |
| Integrate or upgrade an application | [Integration and upgrades](docs/migration.en.md) | [接入与升级](docs/migration.md) |
| Contribute to a component | [Contributing](CONTRIBUTING.en.md) | [贡献指南](CONTRIBUTING.md) |

> This is an independent open-source project. It is not affiliated with, sponsored by, endorsed by,
> or an official project of the United States National Aeronautics and Space Administration. It does
> not use the agency's insignia, logotype, seal or other official visual identifiers. See [NOTICE](NOTICE).

## Core value and architecture

The framework addresses three related needs:

- **One owner for the application lifecycle.** Validate configuration and named resources before
  admitting work; initialize business resources behind a readiness barrier; supervise tasks and
  drain them before releasing their dependencies.
- **Recoverable business workflows.** Keep business writes and messaging intent in one local
  transaction. Use durable Saga state, Inbox deduplication, Outbox delivery, explicit compensation
  and fenced ownership to handle retries, uncertain outcomes and process restarts. A committed Saga
  result can keep converging under separately verified Catalog evidence when command routing is
  unavailable, without reopening new starts, timer claims or readiness.
- **Ordered, bounded execution.** Keep per-key processing order through handler execution,
  acknowledgement and retry while making queue, payload and concurrency limits explicit.

```text
Cargo features + component declarations + final YAML
                         |
             validation and resource preparation
                         |
             business initialization barriers
                         |
           readiness assembly and final revalidation
                         |
           shared activation of managed entry points
                         |
       HTTP / gRPC / message handlers / outbound work
                         |
      close admission -> drain work -> release resources
```

Service mode owns long-running entry points. Batch mode prepares resources before its workload and
cleans up afterwards; it does not publish Service readiness. A bound port is not readiness evidence.
Discovery registration happens after shared activation, and readiness remains unavailable until
registration is confirmed. Ordinary tasks spawned in the startup hook do not implicitly wait for
Ready; application-owned listeners should use `serve_when_ready`.

See [Architecture](docs/architecture.en.md) for the ordering and failure contracts.

## Choose capabilities

The default feature set is empty. Compile the capabilities you need, declare the managed lifecycle
components, and configure their resources explicitly. A Cargo feature and a component string serve
different purposes; for example, `mapper` is a feature, while `"db"` selects database lifecycle ownership.

| Application need | Facade features | Main boundary |
| --- | --- | --- |
| Managed HTTP service | `application,web` | Readiness, listener ownership and bounded shutdown; plaintext HTTP/1 by default |
| gRPC service | `application,grpc` | Generated service adapters, TLS/mTLS, resource limits and draining |
| MySQL transactions and mapping | `application,tx,mapper` | One driver and one datasource per local transaction |
| PostgreSQL transactions and mapping | `application,tx-pgsql,mapper-pgsql` | Explicit PostgreSQL driver and typed APIs |
| Durable Saga | `application,saga-runtime` or `application,saga-runtime-pgsql` | Durable state, Inbox/Outbox, compensation and explicit handling of unknown outcomes |
| Redis and ordered consumption | `application,redis` | Redis ownership and pending entries remain separate from local execution |
| Named local partition runners | `application,partition` | In-process ordering and bounded execution; no durable queue |
| Kafka | `application,kafka` | Broker connectivity, supervised consumption and producer shutdown |
| Transactional messaging | `inbox,outbox` or `inbox-pgsql,outbox-pgsql` | Business facts and delivery intent must share the same datasource |
| SQL observability | `application,mapper` or `application,mapper-pgsql` | Automatic metrics; bounded notifications cannot change transaction results |
| Secrets and outbound TLS | `secret,secret-http` | Explicit credential references and configuration application state |
| Object storage | `object-store` | Bounded single-object operations and integrity checks |

Use the [facade guide](nasa/README.en.md) for the public entry point. The detailed
[feature matrix](nasa/README.md#feature-总表) and
[component index](README.md#组件-readme-索引) are currently in Chinese; crate names, configuration
keys and API identifiers are identical in both languages.

## Start with a small service

Rust 1.94 or newer is required. A basic HTTP service needs:

```toml
[dependencies]
anyhow = "1"
nasa = { version = "2.0.2", default-features = false, features = ["application", "web"] }
```

The [quickstart](docs/quickstart.en.md) supplies the complete manifest, application source, YAML,
startup command and HTTP requests. It uses crates.io dependencies and does not require a local
checkout of this workspace or any database, Redis or Nacos service.

## Strict configuration assembly

`nasa::yml::strict` combines a base file, profile, ordered imports and a fixed environment snapshot
into one bounded candidate. It supports nested defaults, filename globs, target-type binding and
field provenance. Select it before preflight with
`#[nasa::application("log", "web", config = configuration)]` or
`ApplicationSpec::with_config_loader`; see the [configuration factory](napp/README.md#启动前配置工厂)
for its signature. Existing `YmlLoader` calls retain their file-reading scope and do not execute imports.

```yaml
application:
  name: notification-service
log:
  path: ${LOG_PATH:/usr/local/logs/${application.name}}
yml:
  imports:
    - file: /etc/conf/telegram*.yml
      optional: false
    - file: /config/*.yml
      optional: true
config_watch:
  enabled: true
```

Each pattern supports `*` and `?` in the filename only. Files use natural order:
`config-2.yml`, `config-02.yml`, then `config-10.yml`; later values override earlier ones.
Separate pattern groups, exact files and remote documents retain their declared positions.
Environment overlays apply last. A required pattern with no matches fails; optional absence keeps
its observation target. Invalid content, duplicate identities or changing sources reject the candidate.

`${aa.bb.cc}`, `${aa-bb-cc}` and `${AA_BB_CC}` can all fall back to `AA_BB_CC`.
Exact tree paths and raw environment names take precedence; an empty environment value counts as a hit.
Only the selected default branch is evaluated, and environment text is not recursively expanded.
Strict environment/default values stay strings until checked target-type binding, preserving text such
as `001234`. Default whitespace, literal keys and document validation differ from compatibility loading;
follow the [integration guidance](docs/migration.en.md#select-strict-configuration-assembly).

With `application,yml-watch` and the switch above, Service mode observes sources and pattern directories.
Events and a 15-second reconciliation interval feed the same candidate flow; the interval is not an
end-to-end application deadline. Rejection retains the current view. Equal values still reconcile
changed sources. Snapshot versions, `app.config_observation()` revisions and component
`Applied` / `ApplyFailed` / `RestartRequired` states represent separate facts. There is no cross-component
rollback transaction. Frozen imports, environment, connections and trust roots require restart.
See the detailed [naml contract](naml/README.md) and [configuration adapter](config-boot/README.md).

## Configuration and operations

Application reads `zcf/application.yml` relative to the process working directory. The file must
exist, even if its content is `{}`. Configuration precedence is the base file, explicit profile,
ordered local/remote overlays, then `APP__...` environment overrides. Strict imports use the order
described above; compatibility loading keeps its existing reading scope. Credentials belong in the deployment
environment or a secret provider.

Named datasources and clients establish explicit resource identities. An unknown resource reference
fails startup instead of silently selecting another connection. Configuration updates prepare a
candidate before publishing it; a rejected candidate preserves the previous view. A new YAML snapshot
does not mean every component applied it. Frozen resources report `RestartRequired` when changed.

Use readiness for traffic admission and liveness for process supervision. SQL observability separates
method execution, database execution, connection waiting and stream consumption. Query parameters
are off by default; development output requires explicit configuration and redaction. Notification
queues are bounded and non-durable, and notification failures do not change SQL or transaction outcomes.

Detailed operational references are currently in Chinese:

- [Managed capabilities](docs/managed-capabilities.md): features, named plans and ownership.
- [Deployment](docs/deployment.md): configuration, signals, probes and resource limits.
- [Operations](docs/operations.md): health, shutdown outcomes and configuration application state.
- [Saga operations](docs/saga-production.md): roles, authenticated transports, recovery and administration.

## Guarantees and limits

- Transactions are local to one database and datasource. Saga provides recoverable eventual
  consistency, not cross-service ACID, physical exactly-once execution or isolation between workflows.
- Managed Saga result handling freezes the Catalog deadline, revocation identity, security publication
  generation and contract digest for each request. HTTP, gRPC, Kafka and Redis Streams revalidate that
  same authority while the transaction is pending. Losing authority rolls back the whole result
  transaction and preserves the event for retry; a security A→B→A cycle never revives old authority.
- Message delivery can repeat. Business handlers need stable identities and appropriate idempotency.
  An uncertain acknowledgement must not be treated as proof that an effect did not occur.
- Initializer failure prevents readiness but cannot undo facts already committed to an external
  system. Initializers must use transactions or stable idempotency keys where necessary.
- Shutdown has a shared deadline. Forced process termination, blocking code and direct Runner
  cancellation do not guarantee completion of asynchronous cleanup.
- Authentication, tenant ownership, deployment capacity and external infrastructure remain explicit
  application responsibilities. Local executor separation does not isolate shared backend services.
- Compatibility cryptography is for existing protocols. New applications should use the modern
  authenticated-encryption APIs and review the [security guidance](SECURITY.en.md).

## Contributing and security

Read [Contributing](CONTRIBUTING.en.md) for component boundaries, documentation conventions and
the contribution path. Use the private reporting channel in [Security](SECURITY.en.md) for sensitive
findings. Do not include credentials or private deployment data in public discussions.

## License

Choose either [MIT](LICENSE-MIT) or [Apache License 2.0](LICENSE-APACHE).
