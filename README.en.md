# nasa-runtime-rust

[中文](README.md) | [English](README.en.md)

**Managed application lifecycles and reliable business execution for Rust server applications.**

`nasa-runtime-rust` brings configuration validation, resource preparation, business initialization,
traffic admission and ordered shutdown into one application lifecycle. Transactions, Inbox/Outbox,
Saga and ordered message processing support recoverable business workflows. Applications select
capabilities through Cargo features and use their databases and message systems to hold durable facts.
Each component keeps explicit resource, transaction and failure boundaries.

Kafka provides a multi-source event runtime with shared Java `nasa-kafka` wire and recovery contracts:
isolated sources and producer lanes, bounded ordered consumption, same-cluster transactions, and durable
replay, transfer and DLT replay. See [Kafka event runtime](#kafka-event-runtime).

Named Nacos, Consul, Kubernetes and Static/DNS connections isolate registry credentials, evidence
and failures. Select internal calls with `qualifier`, use complete URLs for internet APIs, and keep
registration and shutdown under one owner. The default connection name is lowercase `primary`.

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

Existing Kafka consumers must migrate public APIs and configuration defaults; see the [migration requirements](docs/migration.en.md#migrate-kafka-source-and-configuration).

## Kafka event runtime

Enable `kafka` and use `nasa::kafka`. Managed applications also enable `application`, declare the
`"kafka"` component, and configure either `kafka` or `kafkas.<name>`. `app.kafka(name)` returns a source-bound
handle; an unknown explicit name never selects another source. Java and Rust share event bytes, group
identities, stable transaction IDs and durable task formats, while API syntax remains language-specific.

Each source owns independent producer lanes and consumption owners. A lane tracks actual delivery;
each consumption owner polls its consumer and commits only the safe prefix under its current assignment.
Partition ordering and per-key concurrency share process, source, group, partition and key responsibility
budgets. These limits do not cap total process memory, CPU or broker load.

Same-cluster transactions atomically commit the complete input offsets and approved outputs through a
dedicated producer. An unknown commit protects the owner and requires recovery with the same stable
identity. Finite replay, single-destination transfer and DLT replay persist checkpoints only after
processing or destination confirmation; duplicates remain possible. Database, HTTP, socket and
cross-cluster effects do not become part of the Kafka transaction.

Current readiness, group/slot health, delivery state, a fixed metric catalog and close reports expose
actual responsibility. Admin handles are read-only by default; writes require source-specific permission
and broker ACLs. Transaction operation also requires reachable inter-broker listeners and sufficient
transaction-state replication and ISR. Cancelling a wait never proves that in-flight work has exited.

See the [component architecture](nafka/README.md), [managed lifecycle](napp/README.md#kafka-受管模式),
[consumer macro](nafka-macro/README.md) and [deployment requirements](docs/deployment.md#kafka-来源与事务前置条件).

## Multiple registries and shared discovery ownership

`nadisc-runtime` gives any Rust microservice named, isolated registry connections and an explicit registration lifecycle, without requiring Saga or a database. Connections isolate credentials, service scopes, observations and failures. Adding a connection does not register the application automatically.

A standalone host owns one `DiscoveryOwner`. With napp, `#[nasa::application("web", "discovery")]` manages preparation, startup confirmation, local eligibility, registration withdrawal, application draining and final shutdown. Enable `discovery-runtime` and the required `discovery-nacos`, `discovery-consul` or `discovery-kubernetes` features. Static/DNS remain address sources; each backend retains its own health and permission semantics.

Legacy single-Nacos APIs use the derived `nanacos-sdk` transport. Loss of its control stream invalidates connection health before the existing SDK state machine reconnects. The new owner uses an independent HTTP adapter and does not add SDK registration supervision.

REST clients are selected through `app.discovery()?.rest(Some("business"))`; the default facade uses `primary`, unless a legacy explicit default reference is configured. `grpc-discovery` supplies a managed transport for generated gRPC clients. Empty snapshots, revoked permissions, expired evidence and shutdown prevent new submissions. Already submitted requests still require application authentication, idempotency and uncertain-result handling.

Registry selection uses `qualifier`, with the default connection named `primary` and no additional default configuration field. All five REST Mapping attributes support `qualifier = "business"`; a method overrides its trait default. Inject shared clients with `Client::with_clients(app.discovery()?.rest_clients()?)`. Unqualified calls select primary; missing or unknown selections never fall back to another environment. Existing single-client injection remains available.

`rest-client` enables all five Mapping macros; `rest-discovery` supplies the request builder.
`#[GetMapping(url = "https://dns.google/resolve")]` uses the default HTTP client without registry
lookup. URL methods cannot declare or inherit a qualifier, and macro strings do not expand YAML
placeholders. See the [configuration and internal/public API examples](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/discovery.md) (Chinese).

Each registration has its own reachable IP and mapped port. Saga can select an infrastructure connection independently and use a separate recovery registration for capability renewal and committed results. Recovery eligibility does not authorize new workflow execution. Existing single-Nacos configuration remains available; declaring both configuration roots is rejected.

[Architecture, configuration and lifecycle](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nadisc-runtime/README.md) · [Consul](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nadisc-consul/README.md) · [Kubernetes](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nadisc-kubernetes/README.md)


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
| gRPC service and discovery | `application,grpc`; add `grpc-discovery` for dynamic clients | Generated services and clients, TLS/mTLS, submission gates, resource limits and draining |
| MySQL transactions and mapping | `application,tx,mapper` | One driver and one datasource per local transaction |
| PostgreSQL transactions and mapping | `application,tx-pgsql,mapper-pgsql` | Explicit PostgreSQL driver and typed APIs |
| Durable Saga | `application,saga-runtime` or `application,saga-runtime-pgsql` | Durable state, Inbox/Outbox, compensation and explicit handling of unknown outcomes |
| Redis and ordered consumption | `application,redis` | Redis ownership and pending entries remain separate from local execution |
| Named local partition runners | `application,partition` | In-process ordering and bounded execution; no durable queue |
| Kafka | `application,kafka` | Named sources and lanes, bounded ordered consumption, same-cluster atomic processing, durable replay, recoverable transfer, DLT replay and actual resource drain |
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
nasa = { version = "2.0.3", default-features = false, features = ["application", "web"] }
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
