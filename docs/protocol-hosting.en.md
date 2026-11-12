# Managed protocol hosting and resource boundaries

[中文](protocol-hosting.md)

Applications can keep their own handshake, envelope and authorization model while NASA Runtime owns listeners,
connection admission, initial authentication deadlines and shutdown waiting. Built-in endpoint messaging and
custom protocols are separate paths.

## Responsibilities and startup

napp validates capabilities and configuration, owns startup permission and supervises shutdown. naws owns
listeners, connection capacity and tracked tasks. naws-proto supplies bounded codecs and immutable schema plans.
MQ components own source resources and backend confirmation; nafana supplies bounded observation. Application
identity, routes, subscriptions, side effects and delivery semantics remain application responsibilities.

Enable the nasa `application` and `ws` features, declare the `"ws"` component, and use
`Application::configure_ws` during UserHook to install `ServerBuilder::protocol_handler`.
The asynchronous handler receives a `tokio::net::TcpStream` and `naws::server::ProtocolContext`.
Registration precedes binding and admission; a successful registration is not evidence of an open business route.

An independent host uses `bind_suspended`, or transfers an existing listener to `bind_suspended_on`, then calls
`activate()` after preparation. A transferred listener is not rebound and cannot add a separate `ws_addr`.
External addresses, port mappings and dynamic route catalogs remain host responsibilities.

Custom handlers bypass built-in Message, endpoint, bridge and relay processing and cannot coexist with naws's
built-in cluster transport. `websocket_only()` identifies a dedicated WebSocket listener, without granting route
access. The handler and its transport adapter perform HTTP Upgrade and frame classification.

## Authentication and shutdown

`auth_deadline()` is fixed at accept. Classification, Upgrade and asynchronous authentication share that deadline.
`authenticated()` succeeds only once, before expiry and cancellation. It releases initial authentication capacity
and notifies the connection owner; it does not create application identity or extend any subscription or permit.
A timely confirmation can be observed by a later scheduled owner, while service cancellation still takes priority.

`peer_addr()` reports the actual TCP peer. Proxy identity requires separate trust and validation. Observe
`cancellation()` and register child tasks through `spawn`. Context destruction requests cancellation, but tracked
tasks must actually exit before draining is complete. A cancelled waiter or elapsed shutdown budget does not
prove that a writer or external side effect has stopped.

## Bounded data and observation

`transport::Connection::accept_with_budget` reserves shared bytes before reading or allocation. Buffers, exported
bodies and slices retain that responsibility until their last owner is released. This is not a process RSS limit
or a guarantee of no copies at JSON, MQ, HTTP, WebSocket, TLS or kernel boundaries.

`dynamic::SchemaPlan` compiles a closed catalog into one mode and bounds bytes, depth, expanded fields and dynamic
values. JSON_BYTES, VARINT_TLV, BITPACK_TLV and FAST_FIXED are supported; FAST_FIXED requires non-nullable,
fixed-width primitive fields. Typed derive and dynamic plans have distinct type and JSON mapping contracts.
Compilation neither loads arbitrary code nor grants message permissions.

Declarative applications read a nonempty `APP_CONFIG_FILE`, otherwise `zcf/application.yml`; relative paths use
the process working directory. An invalid selected file denies startup. Environment values are frozen at startup;
observing a new configuration does not establish that resources adopted it.

Register metric catalogs before Ready and reserve worst-case series for labels that appear later. Sampling reads
only the process and explicitly visible or configured resources. Missing, unsupported, failed and stale data stay
distinct. Metrics and successful scrapes do not replace a product's lease or write authority.

socket-center implements its own routing, authorization, subscriptions, realtime delivery and cluster control on
these capabilities. Its service protocol and permission contracts remain outside the generic framework.

[naws](../naws/README.md) · [ProtocolBytes](../naws-proto/README.md) · [napp](../napp/README.md) ·
[Resource observation](../nafana/OBSERVABILITY.md)
