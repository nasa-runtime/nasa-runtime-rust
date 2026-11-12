# Shared receive budget

[简体中文](receive-budget.md)

`transport::Connection::accept_with_budget` accepts an optional `Arc<transport::budget::Budget>`. Multiple connections and listeners can share one capacity. Existing `Connection::accept`, `Reader::receive` and `Inbound` retain their unbudgeted API. An explicitly budgeted connection requires `Reader::receive_bytes`; the old receive method cannot export text without ownership accounting.

`Received` uses `Bytes` for binary, UTF-8 text, Ping and Pong. TCP acquires the complete frame capacity after validating its length and before allocation. WebSocket reserves for standard backend buffers, fragment assembly and temporary copies before reading. Exported messages acquire a separate reservation for their original Vec capacity. Cloning and slicing retain the owner until the final view is dropped.

WebSocket buffers can retain earlier allocations. The connection therefore keeps a high-water allowance: a fixed 16 KiB plus eight times the current wire window. This is a conservative ownership measure, not RSS; kernel buffers, allocator overhead, threads and outbound buffers are outside it. Idle connections do not reserve a maximum-size business message, but consuming a message does not clear the backend high-water charge.

Handshake wire input is limited to 64 KiB; subsequent windows allow `2 × maximum_message + 64 KiB`. A complete text or binary message starts the next window. Control frames do not reset its assembly start, so continuous control traffic also remains subject to a finite window. Each underlying read is at most 4096 bytes; reservations may conservatively exceed the bytes actually read.

Capacity exhaustion rejects immediately. It never waits for other connections and resumes partial input. Timeout, cancellation, malformed input and budget errors require closing the connection. `Budget::used_bytes` exposes current capacity usage. `Budget::retain` accounts for other owned buffers; callers must supply their retained allocation capacity and avoid charging an existing shared allocation again.
