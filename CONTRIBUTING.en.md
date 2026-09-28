# Contributing

[中文](CONTRIBUTING.md) | [English](CONTRIBUTING.en.md)

`nasa-runtime-rust` is a multi-crate workspace for managed application lifecycles and reliable business
execution in Rust server applications. Applications normally depend on the `nasa` facade and select
features. Implementation crates support runtime internals, macro expansion and gradual integration;
public examples should prefer the facade.

Local containers can reproduce protocol and failure semantics but do not establish production
capacity. Target-environment owners approve peak load, replica topology, ACLs, online DDL and disaster
recovery assumptions.

## Contribution path

Run the complete service in the [quickstart](docs/quickstart.en.md), then use the
[architecture guide](docs/architecture.en.md) to identify ownership and failure boundaries.
Contributions can cover configuration, documentation, protocols and application feedback as well as code.

- Open an [issue](https://github.com/nasa-runtime/nasa-runtime-rust/issues) with the public use case,
  expected and observed behavior, and minimal relevant configuration. Keep configuration keys while
  removing credentials, tenant data and private deployment addresses.
- Explain design and compatibility before changing public APIs, configuration or durable semantics.
  Submit changes through a Pull Request targeting `master`.
- Describe the trigger, resulting behavior, caller impact and applicable limits. Keep changes focused
  on the affected components.
- Use the private channel in [Security](SECURITY.en.md) for sensitive findings; do not disclose exploit
  details in public discussions.

The workspace requires Rust 1.94 or newer. Build the component and features relevant to the application:

```bash
cargo check --manifest-path nasa/Cargo.toml --no-default-features --features application,web
cargo fmt --all -- --check
```

The full workspace includes native dependencies. Capabilities such as Kafka may require a C/C++
toolchain, CMake, pkg-config and platform development libraries. Consult the component manifest and
README for its dependencies and features. See [Integration and upgrades](docs/migration.en.md) for
application adoption.

## Documentation conventions

- Maintain Chinese and English project positioning, quickstart, architecture, integration and
  contribution entry points together. Both languages use the same APIs, configuration keys,
  features, dependency versions and failure boundaries. Mark the language of untranslated references.
- Each component has a README covering purpose, feature selection, initialization, YAML, usage and
  limits. The root README introduces the facade and links to component details.
- Public prose describes current capabilities, configuration, invariants and failure consequences.
  Keep source history in Git; do not embed internal work records, milestone labels, assistant attribution
  or temporary diagnostics in public documentation or comments.
- Preserve real API, protocol and configuration identifiers. A version that represents actual
  business state, a definition or configuration remains meaningful and should be described accurately.
- Give database migration files semantic names without date or release-generation prefixes.
- Do not include real secrets, local passwords, private hostnames, internal addresses or business data.

## Source comments

- Keep source comments in Chinese while retaining protocol names, configuration keys and identifiers.
- Function and method documentation includes an accurate `业务作用` description. New or substantially
  changed business functions also document meaningful explicit parameters under `参数说明` and outcomes
  under `返回`; do not document the receiver as a parameter.
- Before critical changes to admission, authority, fencing, promotion, configuration, checkpoints,
  compensation or recovery, explain the business reason, ordering and failure effect.
- Explain business intent and safety boundaries instead of restating the code. Update comments when
  behavior, parameters, outcomes or ordering change, and remove obsolete references.
- Keep comments focused on current semantics without issue-handling labels or internal development tags.

## Code boundaries

- Keep changes within the target component and its explicit integration points.
- Preserve the facade and feature layout; avoid unnecessary cross-crate dependencies.
- When macro output changes the caller contract, update both macro and runtime documentation.
- Explain consistency, commit and failure behavior for cache or transaction changes.
- Document relevant time, queue, batch, concurrency and resource limits for infrastructure capabilities.
- Reject unknown configuration fields, invalid values and conflicting combinations early.
- Keep facade exports, rustdoc, READMEs and examples aligned with public APIs.

## Distribution boundary

Component archives contain product source, public documentation, licenses and required redistribution
files. They do not carry internal interfaces, temporary scripts, credentials, private keys or certificates.
