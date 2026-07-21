# UmaDb .NET Client

A high-performance .NET client for [UmaDB](https://umadb.io/), designed for Dynamic Consistency Boundaries (DCB).

**Documentation:** [C# client guide](https://umadb.io/dotnet-client.html) — connection, concepts, recipes (append/read, consistency boundaries, projections, tracking, idempotent append), API reference, and managing client lifetime.

**F# client:** under development.

---

## Architecture Decision Records (ADR)

### ADR 1: Target Performance & Low Allocations

* **Decision:** Use `ValueTask<T>` for the internal engine and gRPC transport via `protobuf-net.Grpc`.
* **Rationale:** UmaDB handles high-frequency event streams. `ValueTask` gives zero-allocation paths for synchronous completions, reducing GC pressure.

### ADR 2: Triple-Surface API (The Language Bridge)

* **Decision:** Expose different types for C# and F# through a Core library and an Wrappers.
* **Rationale:** C# and F# idiomatic code differs, thus different clients (wrappers) can align better to each idiom. Both sit on the same high-perf engine.

### ADR 3: F# client uses `task` / `taskSeq` (not `Async<'T>`) as its primary async model

* **Decision:** The public F# client (`UmaDb.Fsharp`) uses F# `task` / `taskSeq` (i.e. .NET `Task` / `IAsyncEnumerable`) as its primary async surface, especially for reads and streaming, rather than wrapping everything in `Async<'T>` / `AsyncSeq`.
* **Rationale:**
  - **Direct gRPC alignment:** The generated gRPC stubs expose `Task` and `IAsyncEnumerable<ReadResponse>` with explicit `CancellationToken`. Using `task` / `taskSeq` lets the F# client sit directly on these types without extra adapter layers or duplicate cancellation channels.
  - **Streaming & cancellation semantics:** Long-lived reads/subscriptions are driven by explicit `CancellationToken`s and gRPC stream lifetime. `taskSeq` keeps this 1:1 with the transport; `Async<'T>` would introduce an additional implicit token (`Async.CancellationToken`) that does not map cleanly to gRPC.
  - **Performance on hot paths:** UmaDB clients are used in high-throughput consumers and projections. Every conversion between `Task`/`IAsyncEnumerable` and `Async<'T>` / `AsyncSeq` adds state machines, allocations, and potential context switches. Keeping the public surface on `task` / `taskSeq` minimizes overhead in the streaming path.
  - **F# ergonomics preserved:** F# developers can still opt into `Async<'T>` at their own boundaries (e.g. `Task` → `Async<'T>` via `Async.AwaitTask`, or materializing `taskSeq` to a list), but the core library remains “close to the wire” and does not force that model internally.

### ADR 4: Event metadata is exposed as a keyed map (client), backed by a repeated wire type

* **Decision:** `Event.metadata` is exposed to users as a keyed map — `Map<string, string> option` in the F# client and `IReadOnlyDictionary<string, string>` in the C# client — while the wire/Core representation stays a `repeated MetadataEntry` list (`ResizeArray<MetadataEntry>` in `UmaDb.Core`).
* **Context:** An earlier revision of this client used an ordered list of pairs because the server did not enforce key uniqueness, so a map would have silently dropped duplicate keys on read. As of UmaDB 0.6.6 the server **rejects duplicate metadata keys on append**, so keys are now guaranteed unique in stored events.
* **Rationale:**
  - With uniqueness guaranteed server-side, a keyed map is the natural, most ergonomic representation for keyed lookup data (correlation id, source, etc.) and mirrors the Python client's `dict[str, str]`.
  - The wire type stays `repeated MetadataEntry` (not proto3 `map`) for compatibility; the client converts to/from the map at the edge. Conversion on read is last-wins (`Map.ofSeq` / dictionary indexer) so any legacy event written before the uniqueness check does not throw.
  - Entry order is **not** preserved by these types (F# `Map` is key-sorted; `Dictionary` is unordered). This is acceptable because metadata is keyed lookup data — consumers read by key and do not depend on entry order.

---

## Managing UmaClient

Reuse **one** client per process — gRPC channels are meant to be reused ([Microsoft guidance](https://learn.microsoft.com/en-us/aspnet/core/grpc/performance?view=aspnetcore-8.0)). Creating a client per call adds connection overhead.

- **C#:** Register `UmaClient` as a singleton in DI; the host disposes it on shutdown.
- **F#:** Create one client at startup (e.g. `use client = UmaClient.Connect(...)` in `main`) and pass it where needed.

For DI registration, subscription workers, and full API details, see the [C# client documentation](csharp-client.md).

---

## Performance

* **Trimming:** Compatible with .NET 10 trimming and AOT.
