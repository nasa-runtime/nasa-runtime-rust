# nasa

[中文](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nasa/README.md) | [English](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nasa/README.en.md)

**The facade for managed application lifecycles and reliable business execution in Rust server applications.**

`nasa` is the application entry point to `nasa-runtime-rust`. Select Cargo features and use
`nasa::<module>` APIs; the facade brings in implementation and macro crates as needed. Default
features are empty. Application combines configuration, resource preparation, initialization,
traffic admission and shutdown, while transactions, Inbox/Outbox, Saga and ordered processing
support recoverable workflows.

Start with the complete [HTTP quickstart](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/quickstart.en.md),
then read [Architecture](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/architecture.en.md)
and [Integration and upgrades](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/docs/migration.en.md).

This is an independent open-source project with no affiliation, sponsorship, endorsement or official
relationship with the United States National Aeronautics and Space Administration. The archive includes
the complete statement in `NOTICE`.

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
nasa = { version = "2.0.0", default-features = false, features = ["application", "web"] }
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

## Resource and workflow contracts

- Named MySQL/PostgreSQL, Redis and Kafka resources come from final YAML. Unknown names and driver
  mismatches fail instead of selecting a different source. A transaction stays within one datasource.
- Reliable Saga clients write business facts and start intent in one transaction, and dispatch from
  that same datasource. An event ID does not prove commit or remote completion.
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
