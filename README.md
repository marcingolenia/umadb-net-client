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

* **Decision:** Expose different types for C# and F# through a Core library and an F# Wrapper.
* **Rationale:** C# surface uses `Task` and explicit `CancellationToken`; F# surface uses `Async<T>` with implicit cancellation. Both sit on the same high-perf engine.

---

## Managing UmaClient

Reuse **one** client per process — gRPC channels are meant to be reused ([Microsoft guidance](https://learn.microsoft.com/en-us/aspnet/core/grpc/performance?view=aspnetcore-8.0)). Creating a client per call adds connection overhead.

- **C#:** Register `UmaClient` as a singleton in DI; the host disposes it on shutdown.
- **F#:** Create one client at startup (e.g. `use client = UmaClient.Connect(...)` in `main`) and pass it where needed.

For DI registration, subscription workers, and full API details, see the [C# client documentation](csharp-client.md).

---

## Performance

* **Trimming:** Compatible with .NET 10 trimming and AOT.
