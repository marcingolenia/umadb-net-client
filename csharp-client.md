---
head:
  - - meta
    - name: description
      content: How to use UmaDB with C#
  - - meta
    - name: keywords
      content: UmaDB, client, C#
---

# C# Client for UmaDB

High-performance .NET client for [UmaDB](https://umadb.io/) via gRPC. Implements [DCB](https://dcb.events/specification/) for optimistic concurrency in event-sourced systems.

## Connection

```csharp
using UmaDb.Csharp;

// No TLS
using var client = UmaClient.Connect("localhost", 50051);

// TLS + API key
using var client = UmaClient.Connect("localhost", 50051, caCert: "certs/ca.pem", apiKey: "your-api-key");
```

**Reuse one client** for the app lifetime (gRPC channel reuse). See [Managing UmaClient](#managing-umaclient) below.

---

## Recipes

### 1. Append and read

```csharp
using UmaDb.Csharp;
using UmaDb.Csharp.Messages;
using System.Text.Json;

using var client = UmaClient.Connect("localhost", 50051);

// Your event (e.g. record)
public record OrderCreated(Guid OrderId, decimal Amount);

var payload = new OrderCreated(Guid.NewGuid(), 100.32m);
var evt = new UmaEvent(
    nameof(OrderCreated),
    JsonSerializer.SerializeToUtf8Bytes(payload),
    [$"order-{payload.OrderId}"]
);

var res = await client.AppendAsync([evt]);

var filter = UmaFilter.Where(types: [nameof(OrderCreated)], tags: [$"order-{payload.OrderId}"]);
var read = await client.ReadListAsync(filter);
// read.Events, read.Head
```

### 2. Consistency boundary (read–decide–append)

Same query for read and for the append condition; use the head from the read as `after`:

```csharp
var tag = $"order-{orderId}";
var filter = UmaFilter.Where(types: [nameof(OrderCreated), nameof(OrderShipped)], tags: [tag]);

// Read → build decision model
var read = await client.ReadListAsync(filter);
foreach (var evt in read.Events)
    Apply(evt);  // your logic
var after = read.Head;

// Append with condition: fail if anything matching filter was written after `after`
var newEvt = new UmaEvent(nameof(OrderShipped), data, [tag]);
try
{
    await client.AppendAsync([newEvt], failIfMatch: filter, after: after);
}
catch (UmaDbException.IntegrityException)
{
    // Concurrent write — reload and retry
}
```

### 3. Projections (subscribe)

Run in a worker (e.g. `BackgroundService`), not in HTTP pipeline:

```csharp
public class OrderProjectionService : BackgroundService
{
    private readonly UmaClient _client;
    private readonly IProjectionStore _store;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var sub = _client.Subscribe(
            UmaFilter.Where(types: [nameof(OrderCreated), nameof(OrderShipped)]),
            evt => _store.Upsert(evt),  // idempotent
            stoppingToken
        );
        await Task.Delay(Timeout.Infinite, stoppingToken);
    }
}
```

For full control over the stream, use `ReadAsync` with `filter.WithOptions(o => o.Subscribe = true)`.

### 4. Upstream tracking (exactly-once)

Record the upstream position atomically with the events produced from it:

```csharp
var source = "upstream-orders";
var last = await client.GetTrackingInfoAsync(source);
var next = (last ?? 0) + 1;

// ... process upstream event at next, produce local events ...

await client.AppendAsync(
    events: [localEvent],
    trackingInfo: new UmaTrackingInfo(source, next)
);
```

Recording a position that is not greater than the last one throws `UmaDbException.IntegrityException`.

### 5. Idempotent append (event Id)

Set `UmaEvent.Id` (e.g. `Guid`). Retrying the same append returns the same commit position.

```csharp
var evt = new UmaEvent("OrderCreated", data, [tag], id: Guid.NewGuid());
// ... read to get `after` for your boundary ...
var r1 = await client.AppendAsync([evt], failIfMatch: filter, after: after);
var r2 = await client.AppendAsync([evt], failIfMatch: filter, after: after);
// r1.Position == r2.Position
```

---

## API reference

### UmaClient

| Method | Purpose |
|--------|--------|
| `Connect(host, port, caCert?, apiKey?)` | Build client. TLS when `caCert` is set. Reuse instance. |
| `AppendAsync(events, failIfMatch?, after?, trackingInfo?, ct)` | Append; returns `AppendResponse.Position`. Throws `IntegrityException` when condition fails. |
| `ReadListAsync(filter \| query, ct)` | All matching events and head: `UmaReadResult(Events, Head)`. |
| `ReadAsync(filter \| query, ct)` | `IAsyncEnumerable<UmaReadBatch>`. Each batch: `Events`, `Head`. |
| `Subscribe(filter, onEvent, ct)` | Background subscription; returns `IDisposable`. Handle exceptions in `onEvent`. |
| `GetHeadAsync(ct)` | Last position or `null`. |
| `GetTrackingInfoAsync(source, ct)` | Last tracked position for source, or `null`. |

### UmaFilter

- `UmaFilter.All` — match all.
- `UmaFilter.Where(types: ["A","B"], tags: ["x"])` — types OR’d, tags AND’d per item.
- `.Or(types?, tags?)` — add another OR clause.
- `.WithOptions(o => { o.FromPosition = n; o.Limit = n; o.BatchSize = n; o.Backwards = true; o.Subscribe = true; })` — read options.

### Core types

- **UmaEvent**(`EventType`, `Data` (bytes), `Tags?`, `Id?`) — event to append or read.
- **SequencedUmaEvent**(`Position`, `Event`) — read result.
- **UmaReadResult**(`Events`, `Head?`) — result of `ReadListAsync`: list and head after read.
- **UmaReadBatch**(`Events`, `Head?`) — batch and last known position.
- **UmaTrackingInfo**(`Source`, `Position`) — upstream checkpoint.
- **AppendResponse** — `Position` (commit position).

### Exceptions

`UmaDbException` and derived: `AuthenticationException`, `IntegrityException`, `CorruptionException`, `SerializationException`, `InternalException`, `IoException`.

---

## Managing UmaClient

Use **one** client per process. Creating a client per call adds connection overhead.

**DI (recommended):** register as singleton; host disposes on shutdown.

```csharp
// Config: "UmaDb": { "Host", "Port", "CaCert", "ApiKey" }
builder.Services.Configure<UmaDbOptions>(builder.Configuration.GetSection("UmaDb"));
builder.Services.AddSingleton<UmaClient>(sp =>
{
    var o = sp.GetRequiredService<IOptions<UmaDbOptions>>().Value;
    return UmaClient.Connect(o.Host, o.Port, o.CaCert, o.ApiKey);
});
```

**Without DI:** create at startup, dispose in shutdown path.
