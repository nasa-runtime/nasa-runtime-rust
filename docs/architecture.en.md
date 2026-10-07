# Architecture

[中文](architecture.md) | [English](architecture.en.md)

`nasa-runtime-rust` manages Rust server application lifecycles and combines durable transactions,
messaging and ordered execution for recoverable business workflows. Applications select capabilities
through `nasa`. Application owns resources and tasks; databases and message systems own durable facts.
An in-process state change is never sufficient evidence of a cross-system commit.

## Capability layers

| Layer | Responsibility | Main crates |
| --- | --- | --- |
| Application facade | Features, public modules and macro entry points | `nasa` |
| Application lifecycle | Configuration, named resources, initialization, admission, supervision and shutdown | `napp`, `napp-macro` |
| Durable business operations | Local transactions, Inbox/Outbox, idempotency, Saga and audit | `natx-*`, `nainbox-*`, `naoutbox-*`, `nasaga-*` |
| Messaging and execution | Broker interaction, consumption ownership, local scheduling and capacity | `nadis`, `nafka`, `napart` |
| Protocols and supporting capabilities | HTTP, gRPC, configuration, discovery, secrets and observability | `naweb`, `nagrpc`, `naml` and others |

Components also support explicit assembly. Outside Application, the caller owns startup gates,
health supervision and resource release order. Application builds on Tokio; this project does not
implement another async executor or replace a database or message broker.

## Lifecycle and traffic admission

```text
features + component declarations + local/remote configuration
                            |
           final YAML validation and named resource probes
                            |
           Service startup hook registers business plans
                            |
           Prepare: migration gates and resource assembly
                            |
           initializers: before -> initialize -> after
                            |
           Seal, Ready assembly and task factory construction
                            |
           static checks, critical health and deadline revalidation
                            |
           commit Ready and activation under local state protection
                            |
           managed entry points run; discovery confirmation enables readiness
```

The diagram shows the principal business sequence; individual components can prepare internal
resources in different phases. Initializer dependency edges take precedence over numeric `order`.
All three initializer barriers complete before managed business entry points are activated. Failure
prevents readiness and cleans up acquired resources in reverse activation order. Cleanup cannot undo
external commits; initializers must use transactions or stable idempotency keys where needed.

A bound port proves only ownership of a listening resource. A gRPC listener in `Bound` does not
process RPCs. Discovery registration occurs after shared activation, and dynamic readiness remains
unavailable until registration is confirmed. Readiness depends on locally observed health evidence;
it cannot exclude remote failures that have not yet been observed.

Ordinary `spawn_background` and `spawn_critical` calls in the startup hook do not implicitly wait for
Ready. Use `serve_when_ready` for an application-owned listener. Initializer task factories construct
futures; they must not independently spawn tasks or open listeners.

Batch mode prepares resources and static initializers before the workload, activates selected outbound
resources, and cleans up after the workload. It does not publish Service readiness or accept consumer
and callback plans that require a long-running service.

## Transactions, messages and Saga

```text
one local datasource transaction: Inbox claim + business writes + Outbox intent
                                                   |
                                                 COMMIT
                                                   |
                                  durable dispatcher delivery and retries
                                                   |
                                  downstream Inbox + local business transaction
```

An atomic chain binds to one database driver and datasource. Connecting to multiple databases does
not create a cross-database transaction, and after-commit callbacks do not provide durable dual writes.
Outbox delivery can repeat; consumers need stable message identities and transactional Inbox handling
or an equivalent idempotency strategy.

A Saga Orchestrator commits its Inbox, instance CAS, journal, timers and command Outbox in one
transaction. A Participant commits its Inbox, gate, business facts and result Outbox in another local
transaction. Definition digests and identities are revalidated before claiming deduplication keys.
Uncertain external effects require resolution or operator intervention. An unknown database commit
outcome must not be converted into an ordinary retry.

Committed results use Catalog authority that is separate from command routing. The shared Catalog,
frozen definitions for in-flight instances and producer trust must remain valid. A missing command
route may permit only the original result to converge; it does not reopen starts, timer claims,
management operations or readiness. Each request freezes its evidence deadline, revocation identity,
security publication generation and contract digest. Authority is revalidated across asynchronous
polls, after the instance lock and before transaction handoff; loss rolls back the whole transaction
and preserves the event for retry. A security A→B→A cycle cannot revive old authority.

Fencing capabilities constrain timer claims, renewal and completion; a former owner cannot continue
with expired authority. A reliable client writes business facts and start intent in the transaction
selected by `saga.client.datasource_ref`, and its dispatcher scans that same source. An explicit
conflicting Outbox source prevents startup. An event ID returned inside a transaction proves neither
the outer commit nor remote workflow completion.

These contracts provide recoverable eventual consistency, not cross-service ACID, physical exactly-once
execution or business isolation between Sagas. The detailed [Saga guide](saga-production.md) is in Chinese.

## Ordered consumption and execution isolation

Redis leases, pending entries and fencing determine durable takeover authority. `napart` runners
determine local execution. These are separate responsibilities: a consumer owns its runner set and
does not reuse Application's named runners.

Different Redis sources always have separate execution domains. Within one source,
`partition.executor.scope` selects `source`, `group` or `stream`. Each domain receives a fixed,
non-borrowing share of the source budget. Startup rejects a budget that cannot cover the full topology.
Within one instance and plan, same-key ordering crosses those domains and includes handler execution,
acknowledgement and precise retry. An uncertain ACK is reconciled without rerunning a successful handler.

Cross-process same-key ordering requires producers to route the normalized business key to the same
physical Stream. Delivery remains at least once, so handlers must be idempotent. Domains share Tokio
and the Redis client; local scheduling isolation is not thread, connection or backend isolation.
A standalone `napart` runner provides local execution without message persistence. See the Chinese
[partition consumption reference](../nadis/docs/partition.md).

## Configuration, identity and observability

Strict configuration is selected through a factory before preflight. Three owners have separate roles:
`naml` produces bounded candidates and provenance, `config-boot` supplies Nacos text matching the declared
plan, and `napp` owns material preparation, observation and runtime publication. Business expressions
are evaluated after merging the base file, profile, ordered imports and environment overlays; only
required bootstrap dependencies are evaluated before the first remote fetch. Each filename glob uses
natural order with later files overriding earlier ones; separate declarations keep their positions.

Source permissions are fixed at startup: environment snapshots, imports, patterns, connections and
provider trust roots cannot be expanded by a runtime candidate. Files within a fixed pattern can appear,
disappear or change. Source identities are revalidated around reading and publication. Memory-only
loading uses caller-supplied documents without opening imports. Nested defaults evaluate the selected
branch while preserving empty-environment hits and environment-name aliases.

Source observation, desired configuration and component application are separate states. Equal values
still reconcile source changes and can advance the `config_observation` revision without advancing the
business configuration version. Rejection preserves the current view and observation. Published views
can contain distinct `Applied`, `ApplyFailed` and `RestartRequired` component states; there is no
cross-component rollback transaction. See [naml](../naml/README.md) and the
[integration guidance](migration.en.md#select-strict-configuration-assembly).

Named resources bind to explicit sources. Unknown names or a driver mismatch do not fall back to a
default connection. Configuration candidates are prepared before publication; a rejected candidate
preserves the previous view. Reading new YAML does not prove resources adopted it. Inspect application
state as well; frozen fields report `RestartRequired` when changed.

Authentication supplies a verified identity. Authorization freezes the request's policy and generation;
a message body's claimed identity does not establish trust. Deployments must configure ACLs, TLS/mTLS,
secrets, capacity and tenant boundaries explicitly.

SQL observability separates method execution, database execution, connection waiting and stream
consumption. Parameter output is off by default; development output is bounded and redacted.
Threshold notifications call an application implementation through a bounded queue. Notification
failure does not affect SQL or transaction results, and the queue does not guarantee durable delivery.
Metric export is configured through `grafana.observability`. The framework does not establish production
capacity or infer the target platform's expected instances.

## Shutdown and failure boundaries

Service mode withdraws traffic and closes admission, closes supervised work and initializers, then
runs business shutdown tasks and releases business resources and earlier components. Batch runs
business shutdown tasks after supervised work, releases business resources, then cleans up static
initializers. Every object has one final shutdown owner; all cleanup shares an absolute deadline.

Direct Runner cancellation immediately revokes new resource borrows and global entry points, but does
not guarantee asynchronous cleanup. Supervised futures that remain alive can retain responsibility
for releasing their dependencies. Client handles already copied outside the managed borrowing boundary
are not revoked. `Stopping` does not mean cleanup completed, and an exit code does not replace the
business shutdown report.

Cross-crash guarantees require durable facts. SIGKILL, `panic=abort`, blocking code and tasks that do
not yield are outside asynchronous deadline guarantees. The detailed Chinese references cover
[deployment](deployment.md), [operations](operations.md) and [managed capabilities](managed-capabilities.md).
