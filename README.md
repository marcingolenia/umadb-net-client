# UmaDb .NET Client

A high-performance, low-allocation .NET client for **UmaDB**, designed specifically for Dynamic Consistency Boundaries (DCB).

## Architecture Decision Records (ADR)

### ADR 1: Target Performance & Low Allocations

* **Decision:** Use `ValueTask<T>` for the internal engine and gRPC transport via `protobuf-net.Grpc`.
* **Rationale:** UmaDB handles high-frequency event streams. By using `ValueTask`, we achieve **zero-allocation** paths for synchronous completions (e.g., failed guard clauses or cache hits), reducing GC pressure in high-throughput systems.


### ADR 2: Triple-Surface API (The Language Bridge)

* **Decision:** Expose different types for C# and F# through a Core library and an F# Wrapper.
* **Rationale:**
* **C# Surface (`Task<T>`):** Exposes `Task` and explicit `CancellationToken`. While the engine is `ValueTask`, `Task` is safer for C# users (prevents double-await bugs) and matches standard .NET idioms.
* **F# Surface (`Async<T>`):** Provided via the `UmaDb.Client.FSharp` wrapper. It bridges the high-perf engine to the classic F# `async` block, enabling **implicit cancellation** flow.

---

## Usage Examples

### C# (Explicit & Familiar)
```csharp
// Returns Task<T>, requires explicit CancellationToken
var response = await client.AppendAsync(request, cancellationToken);
```

### F# (Idiomatic & Implicit)
```fsharp
// Returns Async<T>, token is pulled from the ambient async context
async {
    let! response = client.Append(request)
    return response
}

```

---

# Managing UmaClient in your codebase

**Source:** [Performance best practices with gRPC | Microsoft Learn](https://learn.microsoft.com/en-us/aspnet/core/grpc/performance?view=aspnetcore-8.0)

- **Reuse channels:** *"A gRPC channel should be reused when making gRPC calls. Reusing a channel allows calls to be multiplexed through an existing HTTP/2 connection."*

- **Cost of creating a new channel per call:** *"If a new channel is created for each gRPC call then the amount of time it takes to complete can increase significantly. Each call will require multiple network round-trips between the client and the server to create a new HTTP/2 connection: 1. Opening a socket 2. Establishing TCP connection 3. Negotiating TLS 4. Starting HTTP/2 connection 5. Making the gRPC call."*

- **Sharing and concurrency:** *"Channels are safe to share and reuse between gRPC calls."* *"A channel and clients created from the channel can safely be used by multiple threads."* *"Clients created from the channel can make multiple simultaneous calls."*

**Recommendations:**

- **C#:** Register `UmaClient` as a **singleton** in your DI container (see the DI example below). Resolve it where needed; the host will dispose it on shutdown.
- **F#:** Create **one** `UmaClient` at application startup (e.g. in `main` with `use client = UmaClient.Connect(...)`) and **pass it as an argument** to the functions that need it. Dispose only at process exit.

---

## Building projections with `Subscribe`

Run subscriptions in a **dedicated worker** (e.g. a separate process or a `BackgroundService`), not inside the web request pipeline. The web app then reads from the **projection store** (database/cache) that the worker updates.

### Register in Microsoft dependency injection

Requires `Microsoft.Extensions.Options` (and `Microsoft.Extensions.Configuration` if binding from appsettings). Usings: `UmaDb.Csharp`, `UmaDb.Csharp.Messages`, `Microsoft.Extensions.Options`.

```csharp
// appsettings.json: "UmaDb": { "Host": "localhost", "Port": 50051, "CaCert": null, "ApiKey": null }
public class UmaDbOptions
{
    public string Host { get; set; } = "localhost";
    public int Port { get; set; } = 50051;
    public string? CaCert { get; set; }
    public string? ApiKey { get; set; }
}

// In Program.cs or AddServices:
builder.Services.Configure<UmaDbOptions>(builder.Configuration.GetSection("UmaDb"));
builder.Services.AddSingleton<UmaClient>(sp =>
{
    var options = sp.GetRequiredService<IOptions<UmaDbOptions>>().Value;
    return UmaClient.Connect(options.Host, options.Port, options.CaCert, options.ApiKey);
});
builder.Services.AddHostedService<OrderProjectionService>();
```

The host disposes the singleton `UmaClient` on shutdown (singletons that implement `IDisposable` are disposed when the host stops). Register your projection store (e.g. `IProjectionStore`) as needed.

### Worker

Use `using var subscription = ...` inside `ExecuteAsync`. When the host stops, `stoppingToken` is cancelled, you leave the method, and the `using` disposes the subscription.

```csharp
public class OrderProjectionService : BackgroundService
{
    private readonly UmaClient _umaClient;
    private readonly IProjectionStore _store;

    public OrderProjectionService(UmaClient umaClient, IProjectionStore store)
    {
        _umaClient = umaClient;
        _store = store;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        void OnEvent(SequencedUmaEvent evt)
        {
            // Build projection (idempotent), then persist + checkpoint
            _store.Upsert(evt);
        }

        using var subscription = _umaClient.Subscribe(
            UmaFilter.Where(types: [nameof(OrderCreated), nameof(OrderShipped)]),
            OnEvent,
            stoppingToken);

        await Task.Delay(Timeout.Infinite, stoppingToken);
    }
}
```

---

## Performance Notes

* **Trimming:** Fully compatible with `.NET 10` trimming and AOT.