# nasa

[中文](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nasa/README.md) | [English](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nasa/README.en.md)

**The facade for managed application lifecycles and reliable business execution in Rust server applications.**

`nasa` is the application entry point to `nasa-runtime-rust`. Select Cargo features and use
`nasa::<module>` APIs; the facade brings in implementation and macro crates as needed. Default
features are empty. Application combines configuration, resource preparation, initialization,
traffic admission and shutdown, while transactions, Inbox/Outbox, Saga and ordered processing
support recoverable workflows.

The `kafka` feature exposes a multi-source event runtime with Java `nasa-kafka` wire and recovery
contracts, isolated producer lanes, bounded ordered consumption, same-cluster transactions and durable
tasks. See [Kafka facade](#kafka-facade).

Named Nacos, Consul, Kubernetes and Static/DNS connections isolate registry credentials, evidence
and failures. Select internal calls with `qualifier`, use complete URLs for internet APIs, and keep
registration and shutdown under one owner. The default connection name is lowercase `primary`.

The `yml` feature uses `naml` for bounded YAML reading, encoding and configuration assembly. Protocol
documents use [`nasa::yml::yaml`](#external-yaml-documents). Configuration supports nested defaults, ordered filename imports and explainable
candidates. A strict factory connects fixed source permissions, observation and application state
to the same host. See [Strict configuration assembly](#strict-configuration-assembly).

Managed Saga orchestration separates recovery of an already committed result from command routing.
A protected participant can retry the original result under verified Catalog evidence without
reopening starts, timer claims or readiness; the request remains bound to its original security
publication generation and deadline throughout the database transaction.

Start with the complete [HTTP quickstart](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/quickstart.en.md),
then read [Architecture](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/architecture.en.md)
and [Integration and upgrades](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/migration.en.md).

This is an independent open-source project with no affiliation, sponsorship, endorsement or official
relationship with the United States National Aeronautics and Space Administration. The archive includes
the complete statement in `NOTICE`.

Existing Kafka consumers must migrate public APIs and configuration defaults; see the [migration requirements](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/migration.en.md#migrate-kafka-source-and-configuration).

## Kafka facade

Use `nasa::kafka` for source configuration, `KafkaProxy`, producer lanes, consumer traits/macros,
transaction/replay/transfer/DLT plans, checkpoints and diagnostics. With `application,kafka`, obtain
source-bound `KafkaHandle` values through `app.default_kafka()` or `app.kafka(name)`; the `kafka` and
`kafkas` configuration roots are mutually exclusive and explicit names never fall back.

Each source owns its native resources. Consumption owners commit only the safe prefix under current
assignment authority, while lanes retain delivery responsibility until actual completion. Transactions
atomically commit complete input offsets and approved outputs within one Kafka cluster. Unknown commits
stop new work and require the stable identity for recovery. Finite tasks persist checkpoints after
processing or destination confirmation, so cross-resource duplicates remain possible.

Java/Rust interoperability preserves valid group names, event identities, null/empty bytes, ordered
headers and durable formats. Recovery also requires matching physical resources and compatible plans.
External database, HTTP and socket effects are outside the Kafka transaction. Managed Admin views are
read-only by default; writes require explicit permission per source. Readiness, health, metrics and
close reports distinguish admission from actual responsibility.

Optional features provide MySQL shared checkpoints (`kafka-mysql-checkpoint`), Confluent Registry
support (`kafka-schema-registry`) and native TLS/GSSAPI/Zstd capabilities. See the
[component contract](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nafka/README.md) and
[managed lifecycle](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/napp/README.md#kafka-受管模式).

## Multiple registries and shared discovery ownership

`nadisc-runtime` gives any Rust microservice named, isolated registry connections and an explicit registration lifecycle, without requiring Saga or a database. Connections isolate credentials, service scopes, observations and failures. Adding a connection does not register the application automatically.

A standalone host owns one `DiscoveryOwner`. With napp, `#[nasa::application("web", "discovery")]` manages preparation, startup confirmation, local eligibility, registration withdrawal, application draining and final shutdown. Enable `discovery-runtime` and the required `discovery-nacos`, `discovery-consul` or `discovery-kubernetes` features. Static/DNS remain address sources; each backend retains its own health and permission semantics.

Legacy single-Nacos APIs use the derived `nanacos-sdk` transport. Loss of its control stream invalidates connection health before the existing SDK state machine reconnects. The new owner uses an independent HTTP adapter and does not add SDK registration supervision.

REST clients are selected through `app.discovery()?.rest(Some("business"))`; the default facade uses `primary`, unless a legacy explicit default reference is configured. `grpc-discovery` supplies a managed transport for generated gRPC clients. Empty snapshots, revoked permissions, expired evidence and shutdown prevent new submissions. Already submitted requests still require application authentication, idempotency and uncertain-result handling.

Registry selection uses `qualifier`: the default connection is named `primary`, and registration or Saga targets specify `qualifier`, without an additional default configuration field. All five REST Mapping attributes accept `qualifier = "business"`; a method overrides its trait default. Inject the shared set with `Client::with_clients(app.discovery()?.rest_clients()?)`. Calls without a qualifier select `primary`; a directory without it supports explicit named calls only. Unknown names never fall back to another connection. Existing `Client::new(selected_client)` calls remain available for methods without an explicit qualifier; `connection_ref` remains an alias for `qualifier`, while legacy `default_connection` keeps its explicit default override semantics.

`rest-client` enables all five Mapping macros; `rest-discovery` supplies the request builder.
`#[GetMapping(url = "https://dns.google/resolve")]` uses the default HTTP client without registry
lookup. URL methods cannot declare or inherit a qualifier, and macro strings do not expand YAML
placeholders. See the [configuration and internal/public API examples](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/discovery.md) (Chinese).

Each registration has its own reachable IP and mapped port. Saga can select an infrastructure connection independently and use a separate recovery registration for capability renewal and committed results. Recovery eligibility does not authorize new workflow execution. Existing single-Nacos configuration remains available; declaring both configuration roots is rejected.

[Architecture, configuration and lifecycle](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nadisc-runtime/README.md) · [Consul](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nadisc-consul/README.md) · [Kubernetes](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nadisc-kubernetes/README.md)


## Core value and facade architecture

```text
application features + #[nasa::application(...)] + YAML
                              |
                  nasa public modules and macros
                              |
             Application lifecycle and named resources
                              |
       protocol adapters / transactions / messaging / execution
```

The facade connects implementation crates, macro ABI and managed resources into a consistent
dependency graph. Keep the same component identity for transaction contexts, generated protocol
types and lifecycle owners; avoid mixing a local component with a different registry copy.

`application` provides initialization barriers before Ready, supervision and ordered asynchronous
cleanup before dependent resources are released. It builds on Tokio and does not create a new
executor. Independent component assembly remains possible, with ownership transferred to the caller.

## Select capabilities

```toml
[dependencies]
anyhow = "1"
nasa = { version = "2.0.3", default-features = false, features = ["application", "web"] }
```

| Capability | Features | Public entry |
| --- | --- | --- |
| Application lifecycle | `application` | `nasa::Application`, `nasa::application`, `#[nasa::initializer]` |
| HTTP routing | `web` | `nasa::web`; combine with `application` for managed listening |
| MySQL mapping and transactions | `mapper,tx` | `nasa::mapper`, `nasa::tx` |
| PostgreSQL mapping and transactions | `mapper-pgsql,tx-pgsql` | `nasa::mapper::pgsql`, `nasa::tx::pgsql` |
| Redis and ordered consumption | `redis` | `nasa::redis` |
| Local partition execution | `partition` | `nasa::partition` |
| Kafka | `kafka` | `nasa::kafka` |
| Transactional messaging | `inbox,outbox` or their `-pgsql` variants | `nasa::inbox`, `nasa::outbox` |
| Durable Saga | `saga-runtime` or `saga-runtime-pgsql` | `nasa::saga` and role-specific configuration |
| gRPC | `grpc` | `nasa::grpc` |

Features compile capabilities. Component strings assign managed ownership; they are not interchangeable.
For example, declare `"db"` for managed database resources. Declaring `"saga"` implicitly includes DB
and Outbox, while transport remains explicit. `mapper`, `hystrix` and `grafana` are not component strings.
The [complete feature matrix](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nasa/README.md#feature-总表)
is currently in Chinese.

## External YAML documents

Enable `yml` to use the bounded `naml` document API through `nasa::yml::yaml`. No separate YAML library
is required. Kubernetes authentication files and Prometheus scrape configuration use the same API;
application configuration evaluation uses the loaders described in the next section.

```toml
[dependencies]
nasa = { version = "2.0.3", features = ["yml"] }
```

```rust
use nasa::yml::yaml::{from_slice_with_limits, to_string_with_limits, Limits};

let limits = Limits {
    input_bytes: 1024 * 1024,
    output_bytes: 1024 * 1024,
    ..Default::default()
};
let document = from_slice_with_limits(b"service.name: orders\ntoken: ${TOKEN}\n", &limits)?;
let encoded = to_string_with_limits(&document, &limits)?;
```

`document` is a `serde_json::Value`. The key `service.name` and the string `${TOKEN}` remain literal;
reading does not access the environment, execute imports or open files. Mapping, sequence and scalar
roots are accepted within a single UTF-8 document. Duplicate keys, merge keys, unknown tags and exceeded
limits are rejected; alias expansion consumes the budget. Numbers are limited to `i64`/`u64` and finite `f64`.
Encoding preserves values and types, without retaining comments, layout or anchor names. Failures return
`nasa::yml::strict::ConfigError` without a partial document. The caller owns file permissions, atomic
replacement and external reload. See the [naml document contract](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/naml/README.md#外部-yaml-文档读写)
for default budgets and detailed type rules.

## Strict configuration assembly

The `yml` feature exports `nasa::yml::strict`: bounded file or memory loading, nested defaults,
natural-order filename globs, a fixed environment snapshot, target-type binding and field provenance.
With Application, declare `config = configuration` before preflight. The synchronous, zero-argument
factory returns `nasa::yml::strict::Result<nasa::yml::strict::ConfigLoader>`; the standard constructor is
`ConfigLoader::standard()`. Manual entry points use `ApplicationSpec::with_config_loader`.
The [complete factory example](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nasa/README.md#严格配置装配)
uses `application,yml,log,web`; local observation additionally needs `yml-watch`.

`yml.imports` accepts `/etc/conf/telegram*.yml` or `/config/*.yml`. Each group uses natural filename
order (`2`, `02`, `10`), with later files overriding earlier ones. Declarations retain their positions.
Only filename `*` and `?` are supported, without directory recursion. Map imports require explicit
`optional`; required zero matches, invalid content or source changes reject the candidate.

`${LOG_PATH:/usr/local/logs/${application.name}}` supports nested defaults. Empty environment values
still count as hits. `${aa.bb.cc}`, `${aa-bb-cc}` and `${AA_BB_CC}` can fall back to `AA_BB_CC`, after
exact tree and raw environment names. Strict values bind to the requested type; compatibility loading
retains its scalar and whitespace rules. `YmlLoader` does not automatically execute imports.

Service mode with `config_watch.enabled: true` observes actual sources and pattern directories and
performs periodic reconciliation. Equal-value source changes can advance `app.config_observation()`
without changing the business snapshot version. Rejection preserves the current view; component
`Applied`, `ApplyFailed` and `RestartRequired` states remain distinct. Frozen imports, connections and
trust roots require restart. Equal-value reads do not guarantee retrying failed resource installation,
and there is no cross-component rollback transaction. See the
[configuration contract](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/naml/README.md)
and [integration guide](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/migration.en.md#select-strict-configuration-assembly).

## Resource and workflow contracts

- Named MySQL/PostgreSQL, Redis and Kafka resources come from final YAML. Unknown names and driver
  mismatches fail instead of selecting a different source. A transaction stays within one datasource.
- Kafka named sources isolate physical clients, delivery budgets and lifecycle; producer lanes reserve
  separate capacity within each source. Typed consumers provide partition ordering or per-key concurrency
  under process, source, group, partition and key input budgets. These budgets do not cap total RSS or
  arbitrary application allocations. See the [Kafka core contracts](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nafka/README.md#核心价值与边界).
- Kafka subscription groups support multiple owner slots through `consumer.concurrency` and
  `consumer.group_concurrency`. Health aggregates all slots; `group_slots_health` exposes each slot.
  `group_readiness` explains unmet requirements using the current health snapshot; exact partitions
  are supported, and waits for multiple groups recheck every group within one deadline.
  Fixed assignments remain single-slot, and controls across slots are not atomic. See the
  [Kafka capacity and ownership contract](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nafka/README.md#组内多消费者).
- Kafka finite replay and single-destination transfer freeze source ranges and physical resource
  identities. Transfer confirms actual destination delivery before persisting the source checkpoint;
  cancellation retains ownership until in-flight work exits. Stable origin IDs identify duplicates.
  The file backend supports local POSIX recovery; cross-resource atomicity is not provided.
- Reliable Saga clients write business facts and start intent in one transaction, and dispatch from
  that same datasource. An event ID does not prove commit or remote completion.
- Managed result recovery requires the shared Catalog, frozen definitions and producer trust to
  remain valid. HTTP, gRPC, Kafka and Redis Streams freeze and continuously revalidate the request's
  deadline, revocation identity, security publication generation and contract digest. Loss of
  authority rolls back the whole result transaction and preserves the event for retry; A→B→A does
  not revive old authority.
- Redis ordered consumption separates durable takeover from local runners. Sources are independent;
  same-key order within a plan covers handler, ACK and retry. An uncertain ACK is reconciled without
  rerunning a successful handler. Delivery remains at least once.
- Combining `application` with `mapper` or `mapper-pgsql` installs SQL metrics, controlled logging and
  bounded notifications. Applications install `Notify` through
  `nasa::application::notifications::init`; no implementation means no notification delivery.
  Notification failure does not change SQL or transaction outcomes.
- Managed listeners, consumers and outbound entry points share startup activation. Critical resource
  failure prevents readiness. Changes to frozen resource configuration report `RestartRequired`.
- Business cleanup is registered during the startup hook and shares the application shutdown budget.
  A resource has one final close owner. Direct Runner cancellation does not guarantee asynchronous cleanup.

## Limits and further documentation

The framework does not provide cross-service ACID, physical exactly-once execution, automatic tenant
trust, or guaranteed completion under forced process termination. Service readiness is based on
observed evidence and does not prove future backend availability. The default managed Web listener
uses plaintext HTTP/1; `server.http2.enabled` enables h2c prior knowledge, not TLS termination or
HTTP/1 upgrade negotiation.

The detailed [Chinese facade reference](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nasa/README.md)
covers all feature combinations, configuration and public modules. The
[English project guide](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/README.en.md)
links to onboarding, architecture, contribution and security documentation.

## License

Available under MIT or Apache License 2.0, at your option. Both license texts accompany the crate.
