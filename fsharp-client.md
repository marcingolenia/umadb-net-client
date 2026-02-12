---
head:
  - - meta
    - name: description
      content: How to use UmaDB with F#
  - - meta
    - name: keywords
      content: UmaDB, client, F#
---

# F# Client for UmaDB

F# client for [UmaDB](https://umadb.io/) via gRPC. Implements [DCB](https://dcb.events/specification/) for optimistic concurrency in event-sourced systems. The API is **async-only** (tasks and `TaskSeq`); use `task { }` and pipeline-friendly functions. Errors from append conditions and tracking are returned as `Result<int64, IntegrityError>` instead of exceptions.

### Modules

The client is split into these modules under `UmaDb.Client`:

| Module | Use when |
|--------|----------|
| **ClientBuilder** | Building a connection (host, port, TLS, API key) and creating a `UmaClient`. |
| **Operations** | Reading, appending, and subscribing: `readList`, `readWithOptions`, `append`, `subscribeWithCallback`, etc. |
| **Query** | Defining queries and read options: `QueryItem`, `Query`, `QueryOptions` and the `QueryOptions` pipeline (`fromPosition`, `limit`, `subscribe`, …). |
| **Event** | Working with event shapes: `UmaEvent`, `SequencedUmaEvent`, `UmaTrackingInfo`. |
| **Errors** | Handling failures: `UmaDbException` and derived types, `IntegrityError`. |

Typical usage: open **ClientBuilder** and **Operations** to connect and run reads/appends; open **Event** and **Query** when constructing events and queries.

## Installation

Install the **UmaDb.Client.Fsharp** NuGet package. The package targets .NET 10.0 and is compatible with that framework or higher. It depends on `FSharp.Control.TaskSeq` for streaming.

**.NET CLI:**

```bash
dotnet add package UmaDb.Client.Fsharp
```

For other installation methods see the [NuGet page](https://www.nuget.org/packages/UmaDb.Client.Fsharp).

## Connection

Build connection options with the pipeline, then `build` to get a client:

```fsharp
open UmaDb.Client.ClientBuilder
open UmaDb.Client.Operations

use client = connect "localhost" 50051 |> withApiKey "key" |> withTls |> build
```

- **HTTP** (no TLS, no API key)

  ```fsharp
  use client = connect "localhost" 50051 |> build
  ```

- **HTTPS, well-known CA** (no auth)

  ```fsharp
  use client = connect "db.example.com" 443 |> withTls |> build
  ```

- **HTTPS, well-known CA + API key**

  ```fsharp
  use client = connect "db.example.com" 443 |> withApiKey "your-key" |> withTls |> build
  ```

- **HTTPS, self-signed / custom CA** (no API key)

  ```fsharp
  use client = connect "internal.db" 443 |> withCaCert "certs/ca.pem" |> build
  ```

- **HTTPS, self-signed / custom CA + API key**

  ```fsharp
  use client = connect "internal.db" 443 |> withCaCert "certs/ca.pem" |> withApiKey "your-key" |> build
  ```

**Notes:**

- **API key** — Use with TLS. If you set an API key, use `withTls` so the client uses HTTPS.
- **CA cert** — Use when the server certificate is not in the system trust store (self-signed or private CA). Omit for public CAs (e.g. Let's Encrypt). `withCaCert` implies TLS.

**Reuse one client** for the app lifetime (gRPC channel reuse). See [Managing the client](#managing-the-client) below.

---

## Concepts

- **Query** — A filter over the log ([DCB Query](https://dcb.events/specification/)). In F# a query is a list of **QueryItem**s: `{ Types: string list; Tags: string list }`. Each item matches events whose type is in `Types` (or any if empty) **and** whose tags include all of `Tags` (or any if empty). Multiple items are combined with **OR**. Use with `readWithOptions`, `readList`, or `subscribeWithCallback`; pair with **QueryOptions** (position, limit, subscribe, etc.) where needed.
- **Append condition** — `failIfMatch` + `after`. The append fails if the store contains any event matching the query **after** position `after`. Use the **same query** you used to read and the **head** from that read as `withAfter`; then no one else can have written matching events in between. Built with `failIfMatch` and `withAfter` on an `AppendOperation`.
- **Tracking** — `track source position` on an `AppendOperation`. Records “I've processed up to this position on this upstream.” Stored atomically with the events. Positions must be strictly increasing per source.

---

## Recipes

Snippets assume they run inside a `task { }` and that a `CancellationToken` is in scope (e.g. `let ct = CancellationToken.None` or from the request) where used.

### 1. Append and read

```fsharp
open System
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open UmaDb.Client.ClientBuilder
open UmaDb.Client.Operations
open UmaDb.Client.Event
open UmaDb.Client.Query

use client = connect "localhost" 50051 |> build

type OrderCreated = { OrderId: Guid; Amount: decimal }

let orderId = Guid.NewGuid()
let payload = { OrderId = orderId; Amount = 100.32m }
let tag = $"order-{orderId}"
let evt =
    { EventType = "OrderCreated"
      Data = ReadOnlyMemory(JsonSerializer.SerializeToUtf8Bytes(payload))
      Tags = Some [ tag ]
      Id = None }

let! res = appendOperation [ evt ] |> append client CancellationToken.None
// res is Ok position or Error (ErrorMessage _)

let query = [ { Types = [ "OrderCreated" ]; Tags = [ tag ] } ]
let! events, head = readList client query
```

### 2. Consistency boundary (read–decide–append)

Same query for read and for the append condition; use the head from the read as `withAfter`:

```fsharp
let tag = $"order-{orderId}"
let query = [ { Types = [ "OrderCreated"; "OrderShipped" ]; Tags = [ tag ] } ]

// Read → build decision model
let! events, head = readList client query
List.iter (fun evt -> apply evt) events  // your logic
let after = head

// Append with condition: fail if anything matching query was written after `after`
let newEvt = { EventType = "OrderShipped"; Data = data; Tags = Some [ tag ]; Id = None }
let op = appendOperation [ newEvt ] |> failIfMatch query |> withAfter after
match! append client ct op with
| Ok pos -> ()  // committed
| Error (ErrorMessage _) -> ()  // concurrent write — reload and retry
```

### 3. Projections (subscribe)

Run in a worker (e.g. background task), not in an HTTP handler:

```fsharp
let runProjection (client: UmaClient) (store: IProjectionStore) (ct: CancellationToken) = task {
    use _ =
        subscribeWithCallback client ct
            [ { Types = [ "OrderCreated"; "OrderShipped" ]; Tags = [] } ]
            (fun (evt, ct) -> store.UpsertAsync(evt, ct))  // idempotent; events processed sequentially
    do! Task.Delay(Timeout.Infinite, ct)
}
```

For a stream you consume yourself, use `readWithOptions` with `QueryOptions.defaults |> QueryOptions.subscribe` and iterate inside a `task { }`:

```fsharp
task {
    for evt in readWithOptions client ct query (QueryOptions.defaults |> QueryOptions.subscribe) do
        do! handle evt
}
```

For position/limit/backwards, use the same `readWithOptions` and pipe `QueryOptions.fromPosition`, `QueryOptions.limit`, `QueryOptions.backwards`, etc.

### 4. Upstream tracking (exactly-once)

Record the upstream position atomically with the events:

```fsharp
let source = "upstream-orders"
let! last = readTrackingInfo client ct source
let next = (last |> Option.defaultValue 0L) + 1L

// ... process upstream event at next, produce local events ...

let op = appendOperation [ localEvt ] |> track source next
let! result = append client ct op
// If position not strictly increasing, result is Error (ErrorMessage _)
```

### 5. Idempotent append (event Id)

Set `Id` on `UmaEvent` (e.g. `Some (Guid.NewGuid())`). Retrying the same append returns the same commit position.

```fsharp
let evt = { EventType = "OrderCreated"; Data = data; Tags = Some [ tag ]; Id = Some (Guid.NewGuid()) }
let op = appendOperation [ evt ] |> failIfMatch query |> withAfter after
let! r1 = append client ct op
let! r2 = append client ct op
// r1 = r2 (same Ok position)
```

---

## Example: full flow

Read → conditional append → conflict → idempotent retry. On condition failure you would typically reload (`readList`), rebuild your decision model and a new `op` with updated `after`, then append again. If you retry the *same* op (same event `Id`), the server returns the same commit position and no duplicate event is stored.

```fsharp
open System.Threading
open UmaDb.Client.ClientBuilder
open UmaDb.Client.Operations
open UmaDb.Client.Event
open UmaDb.Client.Query

use client = connect "localhost" 50051 |> build
let ct = CancellationToken.None

let tag = "order-123"
let query = [ { Types = [ "OrderCreated"; "OrderShipped" ]; Tags = [ tag ] } ]

// Read, get head
let! events, head = readList client query
List.iter apply events
let after = head

// Append with condition
let evt = { EventType = "OrderShipped"; Data = data; Tags = Some [ tag ]; Id = Some (Guid.NewGuid()) }
let op = appendOperation [ evt ] |> failIfMatch query |> withAfter after
let! posResult = append client ct op
// Concurrent write: same condition + after would give Error → reload and retry.

// Idempotent retry: same op (same event Id) returns the same Ok position; no duplicate event.
let! posResult2 = append client ct op
// posResult = posResult2 (same Ok position)
```

---

## API reference

### ClientBuilder (UmaDb.Client.ClientBuilder)

| Function | Purpose |
|----------|---------|
| `connect (host: string) (port: int)` | Starts a connection builder (HTTP, no TLS). |
| `withTls (builder)` | Use HTTPS (system trust). |
| `withCaCert (path: string) (builder)` | Use custom CA cert (implies TLS). |
| `withApiKey (key: string) (builder)` | Set API key (use with TLS). |
| `build (builder)` | Creates a `UmaClient`. Reuse the instance; dispose when shutting down. |

### Operations (UmaDb.Client.Operations)

| Function | Purpose |
|----------|---------|
| `readList client query` | Returns `Task<SequencedUmaEvent list * int64 option>`. Use for small result sets or building a decision model. For large result sets or cancellation, use `readWithOptions` instead. |
| `readWithOptions client ct query options` | `TaskSeq<SequencedUmaEvent>`. Stream with full read options (position, limit, batch size, backwards, subscribe). |
| `readHead client ct` | `Task<int64 option>`. Last position in the log, or `None` if empty. |
| `readTrackingInfo client ct source` | `Task<int64 option>`. Last tracked position for the upstream source. |
| `appendOperation (events: UmaEvent list)` | Starts an append operation (no condition). Pipe to `failIfMatch` / `withAfter` / `track` then `append`. |
| `failIfMatch query op` | Adds append condition: fail if any event matches the query. Use with `withAfter`. |
| `withAfter (position: int64 option) op` | Sets `after` for the condition. Usually head from read; `None` omits (no events ignored). |
| `track source position op` | Adds upstream tracking; position stored atomically. Positions must increase per source. |
| `append client ct op` | Appends atomically. Returns `Task<Result<int64, IntegrityError>>` — `Ok position` or `Error (ErrorMessage _)` when condition fails or tracking not increasing. |
| `subscribeWithCallback client ct query onEvent` | Subscription; invokes async `onEvent(evt, ct)` for each event (sequential). Returns `IDisposable` — use `use _ = ...` to stop; disposing cancels the subscription and stops delivery. Handle exceptions in `onEvent`. |

### Event (UmaDb.Client.Event)

- **UmaEvent** — `EventType`, `Data` (ReadOnlyMemory&lt;byte&gt;), `Tags` (string list option), `Id` (Guid option).
- **SequencedUmaEvent** — `Position: int64`, `Event: UmaEvent`.
- **UmaTrackingInfo** — `Source: string`, `Position: int64`.

### Query (UmaDb.Client.Query)

- **Query** — `QueryItem list`. Empty list = match all. Each item: `{ Types: string list; Tags: string list }` (types OR'd, tags AND'd per item; items OR'd).
- **QueryOptions** — `FromPosition`, `Limit`, `BatchSize`, `Backwards`, `Subscribe`. Build with `QueryOptions.defaults` and pipe `QueryOptions.fromPosition`, `QueryOptions.limit`, `QueryOptions.batchSize`, `QueryOptions.backwards`, `QueryOptions.subscribe` as needed.

### Operations (UmaDb.Client.Operations) — types

- **AppendOperation** — Record with `Events`, `FailIfMatch`, `After`, `TrackingInfo`. Build with `appendOperation` and pipeline.

### Errors (UmaDb.Client.Errors)

- **IntegrityError** — `ErrorMessage of string` (append condition or tracking violation).

### Exceptions (UmaDb.Client.Errors)

Other failures (network, auth, etc.) are thrown as `UmaDbException` and derived: `AuthenticationException`, `IntegrityException` (thrown only if the failure is surfaced outside the Result-returning API, e.g. by rethrowing after inspecting `Error (ErrorMessage _)`), `CorruptionException`, `SerializationException`, `InternalException`, `IoException`, `CancelledException`.

---

## Managing the client

Use **one** client per process. Creating a client per call adds connection overhead.

- **Reuse channels:** *"A gRPC channel should be reused when making gRPC calls. Reusing a channel allows calls to be multiplexed through an existing HTTP/2 connection."*

- **Cost of creating a new channel per call:** *"If a new channel is created for each gRPC call then the amount of time it takes to complete can increase significantly. Each call will require multiple network round-trips between the client and the server to create a new HTTP/2 connection."*

- **Sharing and concurrency:** Channels and clients created from them are safe to share and use from multiple threads; multiple simultaneous calls are supported.

**Source:** [Performance best practices with gRPC | Microsoft Learn](https://learn.microsoft.com/en-us/aspnet/core/grpc/performance?view=aspnetcore-8.0)

**DI:** register the client as a singleton; let the host dispose on shutdown.

**Without DI:** create at startup (e.g. from config), dispose in your shutdown path (`use` or explicit `Dispose`).
