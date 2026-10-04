# Quickstart

[中文](quickstart.md) | [English](quickstart.en.md)

Use the `nasa` facade to select application capabilities. `#[nasa::application]` owns configuration,
managed resources, traffic admission and shutdown for the service.

## Run a complete service

You need Rust 1.94 or newer. This example uses crates.io dependencies and needs no database, Redis,
Nacos or adjacent source checkout.

```bash
cargo new nasa-hello --bin
cd nasa-hello
mkdir -p zcf
```

Replace `Cargo.toml` with:

```toml
[package]
name = "nasa-hello"
version = "0.1.0"
edition = "2021"
rust-version = "1.94"

[dependencies]
anyhow = "1"
nasa = { version = "2.0.1", default-features = false, features = ["application", "web"] }
```

Replace `src/main.rs` with the following. Source comments retain Chinese to follow the repository's
comment convention; the handler returns a greeting, and the startup hook completes registration
without claiming that the service is already ready.

```rust
/// 业务作用：提供问候端点，展示受管 HTTP 路由。
/// 参数说明：无。
/// 返回：固定问候文本。
#[nasa::web::get_mapping("/hello")]
async fn hello() -> &'static str {
    "Hello from nasa"
}

/// 业务作用：完成服务启动登记，由 Application 继续管理监听与停机。
/// 参数说明：_app 是当前应用的受管上下文。
/// 返回：登记成功，不表示服务已完成接流准备。
#[nasa::application("web")]
async fn main(_app: nasa::Application) -> anyhow::Result<()> {
    Ok(())
}
```

Create `zcf/application.yml`:

```yaml
application:
  name: nasa-hello
  mode: service
  startup_timeout_ms: 30000
  shutdown_timeout_ms: 15000

server:
  host: 127.0.0.1
  port: 8080
  health: true
```

Run from the project root:

```bash
cargo run
```

From another terminal:

```bash
curl --fail http://127.0.0.1:8080/readyz
curl --fail http://127.0.0.1:8080/hello
```

Once ready, `/readyz` returns HTTP 200 and `/hello` returns `Hello from nasa`. `/healthz` represents
process liveness; `/readyz` represents traffic readiness. Binding a port alone does not prove readiness.
If you set `server.context_path`, all three endpoints use that prefix.

Press Ctrl+C once to request normal shutdown. Application stops admitting work and drains the service.
Keep the generated `Cargo.lock`; use `cargo build --locked` for subsequent builds. This example binds
plaintext HTTP to localhost and configures no authentication. Configure a trusted ingress before deployment.

`#[nasa::application]` generates the process entry point; do not combine it with `#[tokio::main]`.
The configuration file is resolved relative to the process working directory and must exist.

## Features, components and configuration

These three choices have distinct roles:

| Choice | Purpose | Example |
| --- | --- | --- |
| Cargo feature | Compile an available capability | `application`, `web`, `tx-pgsql` |
| Component string | Assign lifecycle ownership to Application | `"web"`, `"db"`, `"redis"` |
| YAML and startup plans | Select resource identities, limits and handlers | `datasources.orders`, named consumer plans |

Default facade features are empty. Component strings are validated and put into their canonical
startup order; changing their order in the attribute does not change dependency ordering.
`mapper`, `hystrix` and `grafana` are not component strings.

## Select database capabilities

For MySQL transactions and mapping, extend the facade dependency to:

```toml
nasa = { version = "2.0.1", features = ["application", "web", "tx", "mapper"] }
```

For PostgreSQL:

```toml
nasa = { version = "2.0.1", features = ["application", "web", "tx-pgsql", "mapper-pgsql"] }
```

Declare `"db"` alongside `"web"` in the application attribute. Add a named datasource to the YAML,
and supply the referenced environment variable before startup:

```yaml
datasources:
  orders:
    driver: postgresql
    url: ${APP_POSTGRES_URL}
    max_connections: 8
```

Use `app.datasource("orders").await?` for a MySQL source and
`app.pg_datasource("orders").await?` for a PostgreSQL source. The driver must match. A local
transaction, its Inbox claim, business writes and Outbox append must all use the same driver and
datasource. Enabling both drivers does not create a distributed transaction.

Application can own migration gates, but migration SQL and its registration still belong to the
application. See the [database and migration example](quickstart.md#配置) in Chinese for the detailed
registration contract and [Integration and upgrades](migration.en.md) for data compatibility constraints.

## Add messaging or a workflow

| Need | Facade features | Lifecycle contract |
| --- | --- | --- |
| Redis consumer or AutoPipeline | `application,redis` | Declare `"redis"`; register named plans and handlers |
| Local ordered runner | `application,partition` | Declare `"partition"`; configure a bounded runner |
| Kafka producer and consumer | `application,kafka` | Declare `"kafka"`; configure clients and consumption |
| MySQL Saga | `application,saga-runtime` | Declare `"saga"`; DB and Outbox are included implicitly |
| PostgreSQL Saga | `application,saga-runtime-pgsql` | Declare `"saga"`; select the datasource driver explicitly |

Saga additionally requires role configuration, definitions, participant capabilities and authenticated
transport. A feature alone does not supply these. Use the detailed
[Saga configuration guide](saga-production.md) in Chinese when assembling those contracts.

For a reliable Saga client, enqueue the start intent in the same local transaction as the business
write. The dispatcher must scan that same datasource. An enqueue return value does not prove the
outer transaction committed, and local acceptance does not prove the remote workflow completed.

## Initialization and cleanup

In Service mode, the startup hook registers plans and handlers. Named resources prepared afterwards
should be acquired in an initializer or inside `serve_when_ready`. Initializers complete before
managed entry points are activated. A failed initializer prevents readiness; it does not undo
already committed external effects.

Register application-owned asynchronous cleanup with `register_graceful_shutdown` during the startup
hook. Supervised work is closed before these callbacks, and the callbacks run before startup-hook
resources are released. Do not register a second close operation for an Application-owned resource.
All cleanup shares `application.shutdown_timeout_ms`; forced cancellation does not guarantee that
cleanup futures execute. The [architecture guide](architecture.en.md) explains Service and Batch ordering.

## Common startup failures

| Symptom | What to check |
| --- | --- |
| Configuration file is missing | Start in the directory containing `zcf/application.yml` |
| Address is already in use | Choose a free `server.port` and update the request URLs |
| Unknown component or missing capability | Compare component strings with enabled facade features |
| Unknown YAML field or resource reference | Check spelling and use the exact declared datasource or client name |
| Process exits after its hook | Set `application.mode: service` for a long-running background service |
| A handle rejects work during initialization | Managed entry points may remain closed until shared activation |
| Configuration changes have no effect | Inspect application state; frozen resources require restart |

## Further reading

- [Architecture](architecture.en.md): ownership, ordering and failure boundaries.
- [Integration and upgrades](migration.en.md): dependency identity, configuration and durable state.
- [Facade overview](../nasa/README.en.md): public modules and capability selection.
- [Detailed application recipes](quickstart.md): Chinese examples for Saga, Mapper, SQL notifications,
  migrations and business cleanup.
- [Deployment](deployment.md) and [Operations](operations.md): detailed Chinese operational references.
