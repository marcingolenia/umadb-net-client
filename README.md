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

### ADR 4: Event metadata is an ordered list of key-value pairs, not a map

* **Decision:** `Event.metadata` is represented as an ordered collection of key-value entries across all surfaces — `ResizeArray<MetadataEntry>` in `UmaDb.Core`, `(string * string) list option` in the F# client, and `List<KeyValuePair<string, string>>` in the C# client — not a `Dictionary`/`Map`.
* **Rationale:**
  - On the wire, `metadata` is `repeated MetadataEntry`, not a proto3 `map<string, string>`. Server-side it's backed by `Vec<(String, String)>`, and neither the proto conversion nor the append/read path in UmaDB dedupes or enforces unique keys — duplicate keys are technically valid, similar to HTTP or email headers.
  - The store is shared: events read by this client may have been written by other producers (other language clients, or callers bypassing convenience helpers) that don't guarantee uniqueness. A client-side `Dictionary`/`Map` would silently drop entries for any such event, and would not preserve wire order (F# `Map` is key-sorted, not insertion-ordered).
  - Uniqueness is a convention, not a guarantee, so the client must not assume it or enforce it by construction. Callers who want map-like ergonomics when constructing new events can convert at the call site (e.g. `Map.toList`, `.ToDictionary()`) without weakening the type everyone else relies on for faithful reads.

---

## Managing UmaClient

Reuse **one** client per process — gRPC channels are meant to be reused ([Microsoft guidance](https://learn.microsoft.com/en-us/aspnet/core/grpc/performance?view=aspnetcore-8.0)). Creating a client per call adds connection overhead.

- **C#:** Register `UmaClient` as a singleton in DI; the host disposes it on shutdown.
- **F#:** Create one client at startup (e.g. `use client = UmaClient.Connect(...)` in `main`) and pass it where needed.

For DI registration, subscription workers, and full API details, see the [C# client documentation](csharp-client.md).

---

## Performance

* **Trimming:** Compatible with .NET 10 trimming and AOT.
