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

High-performance .NET client for [UmaDB](https://umadb.io/) via gRPC. Implements [DCB](https://dcb.events/specification/) for optimistic concurrency in event-sourced systems. The client is **async-only** (no synchronous API is provided or planned). If you must call from synchronous code, block on the returned task (e.g. `client.AppendAsync(...).GetAwaiter().GetResult()`); avoid doing that on a thread that has a synchronization context (e.g. UI or legacy ASP.NET) to prevent deadlocks.

## Installation

Install the [UmaDb.Client](https://www.nuget.org/packages/UmaDb.Client/) NuGet package. The package targets .NET 10.0 and is compatible with that framework or higher.

**.NET CLI:**

```bash
dotnet add package UmaDb.Client
```
For other installation methods see [Nuget page](https://www.nuget.org/packages/UmaDb.Client/)

## Connection

Build options fluently, then connect:

```csharp
var options = new UmaClientOptions()
    .WithHost("localhost")
    .WithPort(50051)
    .WithApiKey("key")
    .EnableTls();

using var client = UmaClient.Connect(options);
```

- **HTTP** (no TLS, no API key)

  ```csharp
  UmaClient.Connect(new UmaClientOptions()
      .WithHost("localhost")
      .WithPort(50051))
  ```

- **HTTPS, well-known CA** (no auth)

  ```csharp
  UmaClient.Connect(new UmaClientOptions()
      .WithHost("db.example.com")
      .WithPort(443)
      .EnableTls())
  ```

- **HTTPS, well-known CA + API key**

  ```csharp
  UmaClient.Connect(new UmaClientOptions()
      .WithHost("db.example.com")
      .WithPort(443)
      .WithApiKey("your-key"))
  ```

- **HTTPS, self-signed / custom CA** (no API key)

  ```csharp
  UmaClient.Connect(new UmaClientOptions()
      .WithHost("internal.db")
      .WithPort(443)
      .WithCaCert("certs/ca.pem"))
  ```

- **HTTPS, self-signed / custom CA + API key**

  ```csharp
  UmaClient.Connect(new UmaClientOptions()
      .WithHost("internal.db")
      .WithPort(443)
      .WithCaCert("certs/ca.pem")
      .WithApiKey("your-key"))
  ```

**Notes:**

- **API key** — Use only with TLS. If you set an API key, the client uses HTTPS (system trust when no CA path is given).
- **CA cert** — Only when the server certificate is not in the system trust store (self-signed or private CA). Omit for public CAs (e.g. Let’s Encrypt).

**Reuse one client** for the app lifetime (gRPC channel reuse). See [Managing UmaClient](#managing-umaclient) below.

---

## Concepts

- **Query** — A filter over the log. Built with `UmaQuery.Where(types, tags)` and `.Or(...)`. Each *query item* matches events whose type is in `types` (or any if empty) **and** whose tags include all of `tags` (or any if empty). Multiple items are combined with **OR** (an event matches if any item matches). Use `.WithOptions(...)` to get a <code>UmaQueryWithOptions</code> for read/subscribe APIs that need position, limit, or subscribe.
- **Append condition** — `failIfMatch` + `after`. The append fails if the store contains any event matching the query **after** position `after`. Use the **same query** you used to read and the **head** from that read as `after`; then no one else can have written matching events in between.
- **Tracking** — `UmaTrackingInfo(Source, Position)`. Records “I’ve processed up to this position on this upstream.” Stored atomically with the events you append. Positions must be strictly increasing per source.

---

## Recipes

### 1. Append and read

```csharp
using UmaDb.Client;
using UmaDb.Client.Messages;
using System.Text.Json;

using var client = UmaClient.Connect(new UmaClientOptions().WithHost("localhost").WithPort(50051));

// Your event (e.g. record)
public record OrderCreated(Guid OrderId, decimal Amount);

var payload = new OrderCreated(Guid.NewGuid(), 100.32m);
var evt = new UmaEvent(
    nameof(OrderCreated),
    JsonSerializer.SerializeToUtf8Bytes(payload),
    [$"order-{payload.OrderId}"]
);

var res = await client.AppendAsync([evt]);

var query = UmaQuery.Where(types: [nameof(OrderCreated)], tags: [$"order-{payload.OrderId}"]);
var (events, head) = await client.ReadListAsync(query);
```

### 2. Consistency boundary (read–decide–append)

Same query for read and for the append condition; use the head from the read as `after`:

```csharp
var tag = $"order-{orderId}";
var query = UmaQuery.Where(types: [nameof(OrderCreated), nameof(OrderShipped)], tags: [tag]);

// Read → build decision model
var (events, head) = await client.ReadListAsync(query);
foreach (var evt in events)
    Apply(evt);  // your logic
var after = head;

// Append with condition: fail if anything matching query was written after `after`
var newEvt = new UmaEvent(nameof(OrderShipped), data, [tag]);
try
{
    await client.AppendAsync([newEvt], failIfMatch: query, after: after);
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
        using var sub = _client.SubscribeWithCallback(
            UmaQuery.Where(types: [nameof(OrderCreated), nameof(OrderShipped)]),
            evt => _store.Upsert(evt),  // idempotent
            stoppingToken
        );
        await Task.Delay(Timeout.Infinite, stoppingToken);
    }
}
```

For an async stream of events (e.g. `await foreach`), use `SubscribeAsync(query, ct)`. For full control over the stream (position, limit, etc.), use `ReadAsync` with `query.WithOptions(o => o.Subscribe = true)`.

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
var r1 = await client.AppendAsync([evt], failIfMatch: query, after: after);
var r2 = await client.AppendAsync([evt], failIfMatch: query, after: after);
// r1.Position == r2.Position
```

---

## Example: full flow

One narrative: read → conditional append → conflict → idempotent retry.

```csharp
using var client = UmaClient.Connect(new UmaClientOptions().WithHost("localhost").WithPort(50051));

var tag = "order-123";
var query = UmaQuery.Where(types: ["OrderCreated", "OrderShipped"], tags: [tag]);

// Read, get head
var (events, head) = await client.ReadListAsync(query);
foreach (var evt in events) Apply(evt);
var after = head;

// Append with condition
var evt = new UmaEvent("OrderShipped", data, [tag], id: Guid.NewGuid());
var pos = await client.AppendAsync([evt], failIfMatch: query, after: after);

// Concurrent write: same condition + after would throw IntegrityException → reload and retry.

// Idempotent retry with same event Id returns same position
var pos2 = await client.AppendAsync([evt], failIfMatch: query, after: after);
// pos.Position == pos2.Position
```

---

## API reference

### UmaClient

| Method | Purpose |
|--------|--------|
| `Connect(UmaClientOptions)` | Create client from options. Reuse the instance; dispose when shutting down. |
| `AppendAsync(events, failIfMatch?, after?, trackingInfo?, ct)` | Append; returns `AppendResponse.Position`. Throws `IntegrityException` when condition fails. |
| `ReadListAsync(query \| queryWithOptions, ct)` | Returns `(Events, Head)` tuple. |
| `ReadAsync(query \| queryWithOptions, ct)` | `IAsyncEnumerable<SequencedUmaEvent>`. Stream of events (batching is internal). |
| `SubscribeAsync(query, ct)` | `IAsyncEnumerable<SequencedUmaEvent>`. Subscription stream; use `await foreach` to consume. |
| `SubscribeWithCallback(query, onEvent, ct)` | Background subscription; invokes `onEvent` for each event; returns `IDisposable`. Handle exceptions in `onEvent`. |
| `GetHeadAsync(ct)` | Last position or `null`. |
| `GetTrackingInfoAsync(source, ct)` | Last tracked position for source, or `null`. |

### UmaClientOptions

Fluent options for `Connect`. **WithHost**(`string`), **WithPort**(`int`), **WithApiKey**(`string?`), **WithCaCert**(`string?`), **EnableTls**().

### UmaQuery and UmaQueryWithOptions

- **UmaQuery** — DCB query to filter by types and tags.
  - `UmaQuery.All` — match all.
  - `UmaQuery.Where(types: ["A","B"], tags: ["x"])` — types OR’d, tags AND’d per item.
  - `.Or(types?, tags?)` — add another OR clause.
  - `.WithOptions(o => { ... })` — returns **UmaQueryWithOptions** (query + read options) for `ReadAsync` / `ReadListAsync` / subscribe.
- **UmaQueryWithOptions** — query plus options (position, limit, batch size, backwards, subscribe). Create via `UmaQuery.WithOptions(...)`.

**When to use:** `FromPosition` / `Limit` for resuming or paging; `Subscribe` for live projections; `Backwards` to read from the end.

### Core types

- **UmaEvent**(`EventType`, `Data` (bytes), `Tags?`, `Id?`) — event to append or read.
- **SequencedUmaEvent**(`Position`, `Event`) — read result (each item from `ReadAsync`).
- **UmaTrackingInfo**(`Source`, `Position`) — upstream checkpoint.
- **AppendResponse** — `Position` (commit position).

### Exceptions

`UmaDbException` and derived: `AuthenticationException`, `IntegrityException`, `CorruptionException`, `SerializationException`, `InternalException`, `IoException`.

---

## Managing UmaClient

Use **one** client per process. Creating a client per call adds connection overhead.

- **Reuse channels:** *"A gRPC channel should be reused when making gRPC calls. Reusing a channel allows calls to be multiplexed through an existing HTTP/2 connection."*

- **Cost of creating a new channel per call:** *"If a new channel is created for each gRPC call then the amount of time it takes to complete can increase significantly. Each call will require multiple network round-trips between the client and the server to create a new HTTP/2 connection: 1. Opening a socket 2. Establishing TCP connection 3. Negotiating TLS 4. Starting HTTP/2 connection 5. Making the gRPC call."*

- **Sharing and concurrency:** *"Channels are safe to share and reuse between gRPC calls."* *"A channel and clients created from the channel can safely be used by multiple threads."* *"Clients created from the channel can make multiple simultaneous calls."*

**Source:** [Performance best practices with gRPC | Microsoft Learn](https://learn.microsoft.com/en-us/aspnet/core/grpc/performance?view=aspnetcore-8.0)


**DI (recommended):** register as singleton; host disposes on shutdown.

```csharp
// store config as you wish, for example create UmaDbOptions class and use IOptions
// Config: "UmaDb": { "Host", "Port", "CaCert", "ApiKey" }
builder.Services.Configure<UmaDbOptions>(builder.Configuration.GetSection("UmaDb"));
builder.Services.AddSingleton<UmaClient>(sp =>
{
    var o = sp.GetRequiredService<IOptions<UmaDbOptions>>().Value;
    return UmaClient.Connect(new UmaClientOptions()
        .WithHost(o.Host)
        .WithPort(o.Port)
        .WithCaCert(o.CaCert)
        .WithApiKey(o.ApiKey));
});
```

**Without DI:** create at startup, dispose in shutdown path.
