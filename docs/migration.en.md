# Integration and upgrades

[中文](migration.md) | [English](migration.en.md)

This guide explains how an existing Rust application adopts the dependency and runtime contracts
of `nasa 2.0.0`. The steps depend on the capabilities in use. A version number alone cannot establish
API, schema, message or configuration compatibility. For a new application, start with the
[quickstart](quickstart.en.md).

## Establish dependency identity and features

Prefer selecting capabilities through the facade:

```toml
[dependencies]
nasa = { version = "2.0.0", default-features = false, features = ["application", "web"] }
```

`version = "2.0.0"` is a Cargo compatibility range; the application's `Cargo.lock` fixes the resolved
versions. When adopting registry components, remove `path` dependencies and `[patch.crates-io]`
overrides pointing to local NASA sources while preserving required features. An application using
`sqlx::FromRow` or `sqlx::migrate!` still declares the appropriate direct dependency; the facade does
not implicitly make external crate names available to application code.

Inspect the application's graph for mixed registry, Git and local identities of the same component:

```bash
cargo metadata --format-version 1
cargo tree -d
cargo check --all-targets --locked
```

Build the feature combinations the application actually supports. Unrelated duplicate third-party
versions are not necessarily a problem. Pay particular attention to transaction contexts, macro ABI,
protocol types and resource owners that must share one component identity. Avoid unrelated graph-wide
dependency changes when adopting a particular component.

## Transfer lifecycle ownership

| Existing application responsibility | With Application |
| --- | --- |
| Tokio entry point and signal loop | Use `#[nasa::application]` as the sole process entry point |
| Database, Redis or Kafka connections | Declare sources in final YAML and let the component own them |
| Listener or background consumption loop | Use managed entry points; activate owned listeners through `serve_when_ready` |
| Business resources needed at startup | Register plans in the startup hook; acquire prepared resources in initializers |
| Application-owned asynchronous cleanup | Register `register_graceful_shutdown` in the hook or transfer cleanup to a resource owner; avoid duplicate close operations |

Libraries that do not need Application can assemble components explicitly and own their admission,
observability and cleanup. Decide the single owner of each resource before changing integration.
Copied external client handles do not acquire the revocation guarantees of managed resource borrows.

## Check configuration and entry points

- `zcf/application.yml` must exist and is resolved relative to the process working directory.
- `application.mode` selects Service or Batch. Set `service` explicitly for a long-running background
  process without an inbound component.
- Use `database` for a single source or `datasources` for named sources; the roots are mutually
  exclusive. PostgreSQL requires `driver: postgresql` explicitly.
- Datasource names, Redis qualifiers and Kafka client names are resource identities. Unknown explicit
  references prevent startup.
- A single Redis source has durable identity `primary`; `default` is a lookup alias and does not rename
  persistent keys.
- Use `/readyz` for traffic admission. A bound port or one prepared component is insufficient.
- Connection sources and other frozen fields require restart when changed. New configuration does
  not prove hot application completed.

The detailed [Application contract](../napp/README.md), currently in Chinese, defines fields, defaults
and admission conditions.

## Check durable state and retry semantics

Before changing a deployed binary, establish that its target configuration can read the existing
schema, retained messages, Saga definitions, identities and active business instances. The application
registers business SQL migrations explicitly. Changing a crate version is not authorization to alter
existing durable data automatically.

| Capability | Contract to preserve |
| --- | --- |
| Transactions, Inbox and Outbox | One driver and datasource throughout each atomic chain |
| Reliable Saga client | Business facts, start intent and dispatcher share a transaction domain |
| Saga recovery | Retain referenced definitions and digests; resolve uncertain outcomes instead of blindly retrying |
| Redis consumption | Preserve key routing, group identity and pending-entry responsibility; an uncertain ACK is not proof of non-execution |
| Inbox retention | Keep deduplication markers for at least the actual maximum redelivery horizon |
| Shutdown | Close new admission, wait for in-flight responsibility, then release dependencies |

Establish backup and recovery procedures for the target environment. A rollback must account for
data and protocol readability; replacing a binary alone cannot undo committed schema or business
writes. The detailed [Saga](saga-production.md) and [deployment](deployment.md) references are in Chinese.

## Compatibility information

Component READMEs, rustdoc and protocol files define the current APIs and capability boundaries.
When an integration changes a public API, configuration shape or durable semantics, maintain both
language versions of the guidance and explain the caller's responsibilities. Source history remains
in Git. This guide does not claim that every older version supports a direct upgrade; applications
using components independently must also review those components' assembly contracts.
