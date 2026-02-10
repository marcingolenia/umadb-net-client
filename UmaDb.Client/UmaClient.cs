using Client;
using Grpc.Core;
using Microsoft.FSharp.Core;
using ProtoBuf.Grpc.Client;
using UmaDb.Core;
using UmaDb.Client.Messages;

namespace UmaDb.Client;

/// <summary>
/// Client for reading and appending events to UmaDB via gRPC.
/// Create with <see cref="Connect(UmaClientOptions)"/>; reuse one instance per process (channel reuse). Implements <see cref="IDisposable"/>.
/// </summary>
public sealed class UmaClient(UmaConnection.UmaConnectionResult connection) : IDisposable
{
    private static readonly List<SequencedUmaEvent> EmptyReadBatchEvents = [];
    private static readonly List<string> EmptyTags = [];

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
        UmaReadBatch? firstBatch = null;
        List<SequencedUmaEvent>? results = null;
        long? head = null;
        await foreach (var batch in ReadBatchesAsync(queryWithOptions, ct).ConfigureAwait(false))
        {
            if (firstBatch is null)
            {
                firstBatch = batch;
                head = batch.Head;
                continue;
            }
            if (results is null)
            {
                results = new List<SequencedUmaEvent>(firstBatch.Events.Count + batch.Events.Count);
                results.AddRange(firstBatch.Events);
            }
            results.AddRange(batch.Events);
            head = batch.Head ?? head;
        }
        if (firstBatch is null)
            return (EmptyReadBatchEvents, head);
        if (results is null)
            return (firstBatch.Events, head);
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
    /// Subscribes to events matching the query and yields them as an async stream (like <see cref="ReadAsync(UmaQuery, CancellationToken)"/> but keeps the stream open for new events).
    /// Use this when you want to consume events with <c>await foreach</c>. Use <see cref="SubscribeWithCallback"/> when you prefer a callback on a background task.
    /// When the server exposes a dedicated Subscribe RPC, this method will use it (no backwards/limit); until then it uses the read stream with subscribe enabled.
    /// </summary>
    /// <param name="query">DCB Query to filter by types and tags.</param>
    /// <param name="ct">Cancellation token; when cancelled, the subscription stops.</param>
    /// <returns>Async sequence of <see cref="SequencedUmaEvent"/> (existing and newly appended events).</returns>
    public IAsyncEnumerable<SequencedUmaEvent> SubscribeAsync(UmaQuery query, CancellationToken ct = default) =>
        ReadAsync(new UmaQueryWithOptions(query, new UmaQueryOptions { Subscribe = true }), ct);

    private static ReadOnlyMemory<byte> DataOrEmpty(byte[]? data)
    {
        if (data == null || data.Length == 0)
            return ReadOnlyMemory<byte>.Empty;
        return new ReadOnlyMemory<byte>(data);
    }

    /// <summary>Returns a list suitable for proto Event.Tags: reuses List when possible to avoid copy.</summary>
    private static List<string> TagsForProto(IReadOnlyList<string>? tags)
    {
        if (tags == null || tags.Count == 0)
            return EmptyTags;
        if (tags is List<string> list)
            return list;
        return [.. tags!];
    }

    private async IAsyncEnumerable<UmaReadBatch> ReadBatchesAsync(
        UmaQueryWithOptions queryWithOptions,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var request = new ReadRequest
        {
            Query = queryWithOptions.Query.ToProto(),
            Start = (ulong?)queryWithOptions.Options.FromPosition,
            Backwards = queryWithOptions.Options.Backwards,
            Limit = (uint?)queryWithOptions.Options.Limit,
            Subscribe = queryWithOptions.Options.Subscribe,
            BatchSize = (uint?)queryWithOptions.Options.BatchSize,
        };

        var stream = _service.Read(request, ct);
        await using var enumerator = stream.GetAsyncEnumerator(ct);

        while (true)
        {
            ReadResponse response;
            try
            {
                if (!await enumerator.MoveNextAsync().ConfigureAwait(false))
                    yield break;

                response = enumerator.Current;
            }
            catch (RpcException ex)
            {
                throw UmaDbException.ToUmaDbException(ex);
            }

            yield return MapToBatch(response);
        }
    }

    private static UmaReadBatch MapToBatch(ReadResponse response)
    {
        var rawEvents = response.Events;
        var count = rawEvents?.Count ?? 0;

        if (count == 0)
        {
            return new UmaReadBatch(
                EmptyReadBatchEvents,
                response.Head.HasValue ? (long)response.Head.Value : null);
        }

        var events = new SequencedUmaEvent[count];
        for (var i = 0; i < count; i++)
        {
            var e = rawEvents![i];
            var ev = e.Event;
            events[i] = new SequencedUmaEvent(
                (long)e.Position,
                new UmaEvent(
                    ev.EventType,
                    DataOrEmpty(ev.Data),
                    ev.Tags?.Count > 0 ? ev.Tags : null,
                    string.IsNullOrEmpty(ev.Uuid) ? null :
                        Guid.TryParse(ev.Uuid, out var guid) ? guid : (Guid?)null));
        }

        return new UmaReadBatch(
            events,
            response.Head.HasValue ? (long)response.Head.Value : null);
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
    /// <param name="after">When used with <paramref name="failIfMatch"/>, only events after this position are considered. Use head from <see cref="ReadListAsync(UmaQuery, CancellationToken)"/> or <see cref="GetHeadAsync"/> after a streamed read.</param>
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
            var eventsList = events is IReadOnlyCollection<UmaEvent> col
                ? new List<Event>(col.Count)
                : new List<Event>();
            foreach (var e in events)
            {
                eventsList.Add(new Event
                {
                    EventType = e.EventType,
                    Tags = TagsForProto(e.Tags),
                    Data = e.Data.IsEmpty ? Array.Empty<byte>() : e.Data.ToArray(),
                    Uuid = (e.Id ?? Guid.NewGuid()).ToString()
                });
            }
            var request = new AppendRequest
            {
                Events = eventsList,
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