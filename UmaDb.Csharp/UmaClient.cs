using Client;
using Grpc.Core;
using Microsoft.FSharp.Core;
using ProtoBuf.Grpc.Client;
using UmaDb.Core;
using UmaDb.Csharp.Messages;

namespace UmaDb.Csharp;

/// <summary>
/// Client for reading and appending events to UmaDB via gRPC.
/// Create with <see cref="Connect(UmaClientOptions)"/>; reuse one instance per process (channel reuse). Implements <see cref="IDisposable"/>.
/// </summary>
public sealed class UmaClient(UmaConnection.UmaConnectionResult connection) : IDisposable
{
    private readonly UmaConnection.UmaConnectionResult _connection =
        connection ?? throw new ArgumentNullException(nameof(connection));

    private readonly IDcbService _service = connection.GetCallInvoker().CreateGrpcService<IDcbService>();

    /// <summary>Releases the gRPC channel. Call when the client is no longer needed (e.g. application shutdown).</summary>
    public void Dispose()
    {
        ((IDisposable)_connection).Dispose();
    }

    /// <summary>Returns the sequence position of the last event in the log, or null if the log is empty.</summary>
    /// <param name="ct">Cancellation token.</param>
    public async ValueTask<long?> GetHeadAsync(CancellationToken ct = default)
    {
        try
        {
            var response = await _service.Head(new HeadRequest { _unused = null }, ct);
            return (long?)response.Position;
        }
        catch (RpcException ex)
        {
            throw UmaDbException.ToUmaDbException(ex);
        }
    }

    /// <summary>Returns the last recorded position for the given upstream source, or null if not found.</summary>
    /// <param name="source">Upstream source name (e.g. stream or topic).</param>
    /// <param name="ct">Cancellation token.</param>
    public async ValueTask<long?> GetTrackingInfoAsync(string source, CancellationToken ct = default)
    {
        try
        {
            var response = await _service.GetTrackingInfo(new TrackingRequest { Source = source }, ct);
            return (long?)response.Position;
        }
        catch (RpcException ex)
        {
            throw UmaDbException.ToUmaDbException(ex);
        }
    }

    /// <summary>Reads all events matching the query and returns them plus the head position. Use for small result sets or when building a decision model.</summary>
    /// <param name="query">DCB Query to filter by types and tags.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Tuple of (events, head). Use head as <c>after</c> in conditional appends.</returns>
    public Task<(IReadOnlyList<SequencedUmaEvent> Events, long? Head)> ReadListAsync(UmaQuery query, CancellationToken ct = default) =>
        ReadListAsync(new UmaQueryWithOptions(query, new UmaQueryOptions()), ct);

    /// <summary>Reads all events matching the query and returns them plus the head position.</summary>
    /// <param name="queryWithOptions">DCB Query and options (position, limit, etc.).</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Tuple of (events, head).</returns>
    public async Task<(IReadOnlyList<SequencedUmaEvent> Events, long? Head)> ReadListAsync(UmaQueryWithOptions queryWithOptions, CancellationToken ct = default)
    {
        var results = new List<SequencedUmaEvent>();
        long? head = null;
        await foreach (var batch in ReadBatchesAsync(queryWithOptions, ct).ConfigureAwait(false))
        {
            results.AddRange(batch.Events);
            head = batch.Head ?? head;
        }
        return (results, head);
    }

    /// <summary>Streams events one by one. Use for large result sets or when you need incremental processing. Set <see cref="UmaQueryOptions.Subscribe"/> to keep the stream open for new events.</summary>
    /// <param name="query">DCB Query to filter by types and tags.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Async sequence of <see cref="SequencedUmaEvent"/> (batch boundaries are internal).</returns>
    public IAsyncEnumerable<SequencedUmaEvent> ReadAsync(
        UmaQuery query,
        CancellationToken ct = default) =>
        ReadAsync(new UmaQueryWithOptions(query, new UmaQueryOptions()), ct);

    /// <summary>Streams events one by one. Use <see cref="UmaQueryOptions.Subscribe"/> to keep the stream open for new events.</summary>
    /// <param name="queryWithOptions">DCB Query and options (position, limit, batch size, backwards, subscribe).</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Async sequence of <see cref="SequencedUmaEvent"/>.</returns>
    public async IAsyncEnumerable<SequencedUmaEvent> ReadAsync(
        UmaQueryWithOptions queryWithOptions,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var batch in ReadBatchesAsync(queryWithOptions, ct).ConfigureAwait(false))
        {
            foreach (var evt in batch.Events)
                yield return evt;
        }
    }

    /// <summary>
    /// Streams events matching the query as they become available (subscription). Use <see cref="SubscribeAsync"/> for an async iterator; use this overload when you want to push events to a callback on a background task.
    /// Disposing the returned handle or cancelling <paramref name="ct"/> stops the subscription. Exceptions in the stream or in <paramref name="onEvent"/> are not thrown to the caller—handle them inside <paramref name="onEvent"/>.
    /// </summary>
    /// <param name="query">DCB Query to filter by types and tags.</param>
    /// <param name="onEvent">Callback for each event. Should be idempotent when building projections.</param>
    /// <param name="ct">When cancelled, the subscription stops.</param>
    /// <returns>Disposable that stops the subscription when disposed.</returns>
    public IDisposable SubscribeWithCallback(UmaQuery query, Action<SequencedUmaEvent> onEvent, CancellationToken ct = default)
    {
        var queryWithOpt = new UmaQueryWithOptions(query, new UmaQueryOptions { Subscribe = true });
        var stopCts = new CancellationTokenSource();
        var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, stopCts.Token);
        var token = linkedCts.Token;

        _ = Task.Run(async () =>
        {
            try
            {
                await foreach (var evt in ReadAsync(queryWithOpt, token).ConfigureAwait(false))
                    onEvent(evt);
            }
            finally
            {
                linkedCts.Dispose();
            }
        }, token);

        return new SubscriptionHandle(stopCts);
    }

    /// <summary>
    /// Subscribes to events matching the query and yields them as an async stream (like <see cref="ReadAsync"/> but keeps the stream open for new events).
    /// Use this when you want to consume events with <c>await foreach</c>. Use <see cref="SubscribeWithCallback"/> when you prefer a callback on a background task.
    /// When the server exposes a dedicated Subscribe RPC, this method will use it (no backwards/limit); until then it uses the read stream with subscribe enabled.
    /// </summary>
    /// <param name="query">DCB Query to filter by types and tags.</param>
    /// <param name="ct">Cancellation token; when cancelled, the subscription stops.</param>
    /// <returns>Async sequence of <see cref="SequencedUmaEvent"/> (existing and newly appended events).</returns>
    public IAsyncEnumerable<SequencedUmaEvent> SubscribeAsync(UmaQuery query, CancellationToken ct = default) =>
        ReadAsync(new UmaQueryWithOptions(query, new UmaQueryOptions { Subscribe = true }), ct);

    private async IAsyncEnumerable<UmaReadBatch> ReadBatchesAsync(
        UmaQueryWithOptions queryWithOptions,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var enumerable = _service.Read(new ReadRequest
        {
            Query = queryWithOptions.Query.ToProto(),
            Start = (ulong?)queryWithOptions.Options.FromPosition,
            Backwards = queryWithOptions.Options.Backwards,
            Limit = (uint?)queryWithOptions.Options.Limit,
            Subscribe = queryWithOptions.Options.Subscribe,
            BatchSize = (uint?)queryWithOptions.Options.BatchSize,
        }, ct).Select(response => new UmaReadBatch(
            (response.Events ?? []).Select(e => new SequencedUmaEvent(
                (long)e.Position,
                new UmaEvent(
                    e.Event.EventType,
                    e.Event.Data ?? [],
                    e.Event.Tags?.ToArray(),
                    string.IsNullOrEmpty(e.Event.Uuid) ? null : Guid.TryParse(e.Event.Uuid, out var guid) ? guid : null))).ToList(),
            response.Head.HasValue ? (long)response.Head.Value : null));

        var enumerator = enumerable.GetAsyncEnumerator(ct);
        await using (enumerator)
        {
            while (true)
            {
                UmaReadBatch current;
                try
                {
                    if (!await enumerator.MoveNextAsync())
                        break;
                    current = enumerator.Current;
                }
                catch (RpcException ex)
                {
                    throw UmaDbException.ToUmaDbException(ex);
                }
                yield return current;
            }
        }
    }

    private sealed class SubscriptionHandle(CancellationTokenSource cts) : IDisposable
    {
        public void Dispose()
        {
            cts.Cancel();
            cts.Dispose();
        }
    }

    /// <summary>
    /// Appends events atomically. Optionally use <paramref name="failIfMatch"/> and <paramref name="after"/> for optimistic concurrency; optionally include <paramref name="trackingInfo"/> to record an upstream position.
    /// </summary>
    /// <param name="events">Events to append. Must not be empty.</param>
    /// <param name="failIfMatch">If set, append fails when the log contains any matching event after <paramref name="after"/> (use same query and head from read).</param>
    /// <param name="after">When used with <paramref name="failIfMatch"/>, only events after this position are considered. Use head from <see cref="ReadListAsync"/> or <see cref="GetHeadAsync"/> after a streamed read.</param>
    /// <param name="trackingInfo">Optional upstream checkpoint; stored atomically with the events. Positions must increase per source.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Response containing the commit position of the last appended event.</returns>
    /// <exception cref="UmaDbException.IntegrityException">Append condition failed or tracking position not strictly increasing.</exception>
    public async ValueTask<AppendResponse> AppendAsync(
        IEnumerable<UmaEvent> events,
        UmaQuery? failIfMatch = null,
        long? after = null,
        UmaTrackingInfo? trackingInfo = null,
        CancellationToken ct = default)
    {
        try
        {
            var request = new AppendRequest
            {
                Events = [.. events.Select(e => new Event
                {
                    EventType = e.EventType,
                    Tags = e.Tags?.ToList() ?? [],
                    Data = e.Data.ToArray(),
                    Uuid = (e.Id ?? Guid.NewGuid()).ToString()
                })],
                Condition = failIfMatch != null
                    ? new AppendCondition
                    {
                        FailIfEventsMatch = failIfMatch.ToProto(),
                        After = (ulong?)after
                    }
                    : null,
                TrackingInfo = trackingInfo != null
                    ? new TrackingInfo
                    {
                        Source = trackingInfo.Source,
                        Position = (ulong)trackingInfo.Position
                    }
                    : null
            };

            return await _service.Append(request, ct);
        }
        catch (RpcException ex)
        {
            throw UmaDbException.ToUmaDbException(ex);
        }
    }

    /// <summary>Creates a client from the given options. Reuse the returned instance; dispose when shutting down.</summary>
    /// <exception cref="ArgumentException">Host is null or empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Port is not between 1 and 65535.</exception>
    public static UmaClient Connect(UmaClientOptions options)
    {
        if (options == null)
            throw new ArgumentNullException(nameof(options));
        if (string.IsNullOrWhiteSpace(options.Host))
            throw new ArgumentException("Host cannot be empty.", nameof(options));
        if (options.Port is < 1 or > 65535)
            throw new ArgumentOutOfRangeException(nameof(options), options.Port, "Port must be between 1 and 65535.");

        static FSharpOption<string> Opt(string? s) =>
            string.IsNullOrEmpty(s) ? FSharpOption<string>.None : FSharpOption<string>.Some(s);

        var conn = UmaConnection.UmaConnection.create(
            options.Host!,
            options.Port,
            Opt(options.CaCertPath),
            Opt(options.ApiKey),
            options.UseTls);
        return new UmaClient(conn);
    }
}