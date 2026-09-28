# Security

[中文](SECURITY.md) | [English](SECURITY.en.md)

## Scope

Security maintenance covers all components on the current protected branches. A report should identify
the enabled features, deployment boundaries and reachable network interfaces. Source state and
resolution history are recorded in Git and security advisories.

## Private reporting

Use the repository's
[private vulnerability reporting](https://github.com/nasa-runtime/nasa-runtime-rust/security/advisories/new)
channel. Do not disclose technical details publicly before a remedy is available. If the private
channel is unavailable, open an ordinary issue containing no technical details, credentials,
reproduction instructions or affected targets, and ask maintainers to restore private reporting.

Sensitive findings include:

- Authentication, authorization or tenant-isolation bypass at WebSocket, REST, scheduling or
  administrative entry points.
- Secrets, tokens, connection strings or business payloads exposed through logs, metrics, examples or documentation.
- SQL injection or uncontrolled dynamic fragments in mapping macros.
- Cache behavior that exposes uncommitted data or crosses tenant boundaries.
- External effects occurring before the business transaction commits.
- Redis, Kafka, Stream or discovery traffic routed to the wrong identity or destination.
- Unbounded queues, tasks, buffers, payloads, image dimensions or connections that permit resource exhaustion.
- Bypassed Saga identity, participant gates, administrative permissions, timer fencing or outcome handling.

## Security defaults and boundaries

- Mapper uses bound SQL parameters; raw SQL fragments require an explicit allowlist capability.
- Shared-cache behavior inside transactions must be explicit so uncommitted data does not leak into L2.
- WebSocket configuration bounds connections, unauthenticated clients, payloads, send queues and handler concurrency.
- Redis Stream, Kafka and partition failures remain visible to callers rather than silently dropping work.
- Configuration rejects unknown fields, missing required values and conflicts, and redacts credentials.
- Compatibility cryptography supports controlled integration with existing protocols. New applications
  should use modern authenticated encryption. Legacy private-key decryption is excluded from the stable
  capability combination and requires both explicit compilation and runtime risk controls.
- Saga producer identity comes from broker ACLs, mTLS principals or signatures covering the complete
  envelope; body fields cannot establish identity.
- Redis Stream signatures cover stream, event identity, payload and trace presence. Source Stream,
  dead-letter destination and marker must share a Redis Cluster slot.
- gRPC peer identity requires verified mTLS or an end-to-end signature; self-declared service metadata
  is not identity evidence.
- Saga administrative identities and permissions come from trusted authentication context; request
  bodies cannot replace them. Participant authorization binds workflow, definition version, digest,
  step and orchestrator rather than a global allowlist.
- A deterministic command rejection is recorded durably before advancing its source Outbox or offset.
  HTTP entry points isolate authenticators, replay protection, capacity and metrics by producer and path.
  Multiple replicas require a shared strongly consistent nonce claim.
- `Unknown` requires explicit typed resolution. A timeout or an absent record must not be presented
  as a confirmed failure.
- Timer owners are replica-unique and restart-stable. A fencing capability also binds the runtime
  instance nonce and claim batch; an arbitrary string cannot grant that authority.

## Dependencies

Supply-chain checks use:

```bash
cargo deny check
```

Any temporary advisory exception needs a retention reason, analysis of reachable affected paths,
existing isolation and explicit conditions for removing the exception. High-risk paths require
effective feature gates, runtime controls, permissions or removal; a documentation warning alone
does not reduce reachability.
