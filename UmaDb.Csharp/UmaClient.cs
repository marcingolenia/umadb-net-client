using Client;
using Grpc.Core;
using Microsoft.FSharp.Core;
using ProtoBuf.Grpc.Client;
using UmaDb.Core;
using UmaDb.Csharp.Messages;

namespace UmaDb.Csharp;

/// <summary>
/// Client for reading and appending events to UmaDB via gRPC.
/// Create with <see cref="Connect"/>; reuse one instance per process (channel reuse). Implements <see cref="IDisposable"/>.
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

    /// <summary>Reads all events matching the filter and returns them plus the head position. Use for small result sets or when building a decision model.</summary>
    /// <param name="filter">Filter by event types and tags.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Tuple of (events, head). Use head as <c>after</c> in conditional appends.</returns>
    public Task<(IReadOnlyList<SequencedUmaEvent> Events, long? Head)> ReadListAsync(UmaFilter filter, CancellationToken ct = default) =>
        ReadListAsync(new UmaQuery(filter, new UmaQueryOptions()), ct);

    /// <summary>Reads all events matching the query and returns them plus the head position.</summary>
    /// <param name="query">Filter and options (position, limit, etc.).</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Tuple of (events, head).</returns>
    public async Task<(IReadOnlyList<SequencedUmaEvent> Events, long? Head)> ReadListAsync(UmaQuery query, CancellationToken ct = default)
    {
        var results = new List<SequencedUmaEvent>();
        long? head = null;
        await foreach (var batch in ReadAsync(query, ct).ConfigureAwait(false))
        {
            results.AddRange(batch.Events);
            head = batch.Head ?? head;
        }
        return (results, head);
    }

    /// <summary>Streams events in batches. Use for large result sets or when you need incremental processing.</summary>
    /// <param name="filter">Filter by event types and tags.</param>
    /// <param name="ct">Cancellation token.</param>
    public IAsyncEnumerable<UmaReadBatch> ReadAsync(
        UmaFilter filter,
        CancellationToken ct = default) =>
        ReadAsync(new UmaQuery(filter, new UmaQueryOptions()), ct);

    /// <summary>
    /// Subscribes to events matching the filter; invokes <paramref name="onEvent"/> for each event on a background task.
    /// Disposing the returned handle or cancelling <paramref name="ct"/> stops the subscription. Exceptions in the stream or in <paramref name="onEvent"/> are not thrown to the caller—handle them inside <paramref name="onEvent"/>.
    /// </summary>
    /// <param name="filter">Filter by event types and tags.</param>
    /// <param name="onEvent">Callback for each event. Should be idempotent when building projections.</param>
    /// <param name="ct">When cancelled, the subscription stops.</param>
    /// <returns>Disposable that stops the subscription when disposed.</returns>
    public IDisposable Subscribe(UmaFilter filter, Action<SequencedUmaEvent> onEvent, CancellationToken ct = default)
    {
        var query = new UmaQuery(filter, new UmaQueryOptions { Subscribe = true });
        var stopCts = new CancellationTokenSource();
        var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, stopCts.Token);
        var token = linkedCts.Token;

        _ = Task.Run(async () =>
        {
            try
            {
                await foreach (var batch in ReadAsync(query, token).ConfigureAwait(false))
                {
                    foreach (var evt in batch.Events)
                        onEvent(evt);
                }
            }
            finally
            {
                linkedCts.Dispose();
            }
        }, token);

        return new SubscriptionHandle(stopCts);
    }

    /// <summary>Streams events in batches. Use <see cref="UmaQueryOptions.Subscribe"/> to keep the stream open for new events.</summary>
    /// <param name="query">Filter and options (position, limit, batch size, backwards, subscribe).</param>
    /// <param name="ct">Cancellation token.</param>
    public async IAsyncEnumerable<UmaReadBatch> ReadAsync(
        UmaQuery query,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var enumerable = _service.Read(new ReadRequest
        {
            Query = query.Filter.ToProto(),
            Start = (ulong?)query.Options.FromPosition,
            Backwards = query.Options.Backwards,
            Limit = (uint?)query.Options.Limit,
            Subscribe = query.Options.Subscribe,
            BatchSize = (uint?)query.Options.BatchSize,
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
    /// <param name="failIfMatch">If set, append fails when the log contains any matching event after <paramref name="after"/> (use same filter and head from read).</param>
    /// <param name="after">When used with <paramref name="failIfMatch"/>, only events after this position are considered. Use head from <see cref="ReadListAsync"/> or <see cref="UmaReadBatch.Head"/>.</param>
    /// <param name="trackingInfo">Optional upstream checkpoint; stored atomically with the events. Positions must increase per source.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Response containing the commit position of the last appended event.</returns>
    /// <exception cref="UmaDbException.IntegrityException">Append condition failed or tracking position not strictly increasing.</exception>
    public async ValueTask<AppendResponse> AppendAsync(
        IEnumerable<UmaEvent> events,
        UmaFilter? failIfMatch = null,
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

    /// <summary>
    /// Creates and connects a client. Use TLS by passing <paramref name="caCert"/> (path to PEM); optionally set <paramref name="apiKey"/> for authentication.
    /// Reuse the returned instance for the lifetime of the process; dispose when shutting down.
    /// </summary>
    /// <param name="host">Server hostname.</param>
    /// <param name="port">Server port.</param>
    /// <param name="caCert">Optional path to CA certificate (PEM). When set, uses TLS.</param>
    /// <param name="apiKey">Optional API key when server requires authentication.</param>
    public static UmaClient Connect(
        string host,
        int port,
        string? caCert = null,
        string? apiKey = null)
    {
        var conn = UmaConnection.UmaConnection.create(
            host,
            port,
            caCert is not null ? FSharpOption<string>.Some(caCert) : FSharpOption<string>.None,
            apiKey is not null ? FSharpOption<string>.Some(apiKey) : FSharpOption<string>.None);
        return new UmaClient(conn);
    }
}