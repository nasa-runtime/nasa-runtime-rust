# nanacos-sdk

Nacos configuration and naming transport SDK for the legacy single-Nacos interfaces of `nanacos`. It preserves connection identity, ephemeral registrations and subscriptions across reconnection. A failed or terminated bidirectional control stream invalidates connection health; the existing reconnect state machine establishes a fresh session.

Derived from [nacos-sdk 0.8.3](https://crates.io/crates/nacos-sdk/0.8.3), retaining its public types, feature names and Rust library name `nacos_sdk`. The original implementation is provided by nacos-group and its contributors under Apache-2.0. See [NOTICE](NOTICE) and [LICENSE](LICENSE). This independently maintained component is not an upstream distribution.

## Integration and ownership

Applications normally enable `nacos-sdk` through `nanacos` or the NASA facade. New multi-registry owners use the HTTP adapter in `nanacos::managed`; both owners must not supervise the same registration target. Direct dependencies use package `nanacos-sdk` version `1.0.0` with library name `nacos_sdk`.

Default features are `config`, `naming` and `auth-by-http`. Optional `tls`, `auth-by-aliyun` and `tracing-log` provide TLS, RAM signing and the SDK tracing subscriber. Configure `api::props::ClientProps` and construct `ConfigServiceBuilder` or `NamingServiceBuilder`. Use separate services for distinct credentials or namespaces.

Configuration and naming services own separate SDK sessions. Qualifiers belong to the upper connection directory and do not switch the namespace or credentials of an existing session. Reconnection restores registrations and subscriptions using their fixed identities without a second heartbeat scheduler. It cannot undo submitted remote effects or provide database fencing. Explicit deregistration and shutdown deadlines belong to `nanacos` and its host; crashes, unreachable deregistration and endpoint reuse retain their ownership limitations.

## Disk cache

Writes await directory preparation and exclusively create a random temporary file for each attempt. The complete contents are synced before atomically replacing the destination. Concurrent saves for the same key, keys with different extensions and multiple store instances do not share temporary files. Ordinary I/O failures retry within a fixed bound and clean up only the current attempt's temporary file. Cancellation or a process crash can leave temporary files, which loading skips. Concurrent cache updates do not provide business-version ordering, and disk contents do not establish current server configuration or registration eligibility.

Authentication, request and connection failures are observable through return values and tracing logs.

A fixed JSON tuple of module and namespace selects a SHA-256 directory; a SHA-256 filename locates each logical key. Configuration identities are reversible JSON arrays of dataId, group and namespace. Separators, Unicode, absolute-looking values and `.tmp` suffixes remain logical characters. Every record retains its original scope and key, which are checked against its filename. Corrupt records, mismatched identities and symbolic links are neither loaded, overwritten nor deleted.

The deployment must exclusively control the cache root. Access canonicalizes the root, rejects symbolic links for scope directories and records, and rechecks confinement before replacement or deletion. This prevents path traversal through business identifiers; it does not isolate a hostile local process that can modify the root itself. Use operating-system permissions or isolated volumes for that threat. The previous raw-filename layout is neither loaded nor migrated automatically: contents are reacquired from Nacos, and old files remain for the operator to handle. Disk failure affects the cache only; it cannot restore revoked in-memory state or prove offline eligibility.

## License

Apache-2.0. [中文说明](README.md).
