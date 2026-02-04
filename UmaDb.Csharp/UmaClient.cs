using Client;
using Grpc.Core;
using Microsoft.FSharp.Core;
using ProtoBuf.Grpc.Client;
using UmaDb.Core;
using UmaDb.Csharp.Messages;

namespace UmaDb.Csharp;

public sealed class UmaClient(UmaConnection.UmaConnectionResult connection) : IDisposable
{
    private readonly UmaConnection.UmaConnectionResult _connection =
        connection ?? throw new ArgumentNullException(nameof(connection));

    private readonly IDcbService _service = connection.GetCallInvoker().CreateGrpcService<IDcbService>();
    
    public void Dispose()
    {
        ((IDisposable)_connection).Dispose();
    }
    
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

    public Task<List<SequencedUmaEvent>> ReadListAsync(UmaFilter filter, CancellationToken ct = default) =>
        ReadListAsync(new UmaQuery(filter, new UmaQueryOptions()), ct);

    public async Task<List<SequencedUmaEvent>> ReadListAsync(UmaQuery query, CancellationToken ct = default)
    {
        var results = new List<SequencedUmaEvent>();
        await foreach (var response in ReadAsync(query, ct).ConfigureAwait(false))
        {
            results.AddRange(response.Events);
        }
        return results;
    }

    public IAsyncEnumerable<UmaReadBatch> ReadAsync(
        UmaFilter filter,
        CancellationToken ct = default) =>
        ReadAsync(new UmaQuery(filter, new UmaQueryOptions()), ct);

    /// <summary>
    /// Subscribes to the event store and invokes <paramref name="onEvent"/> for each event.
    /// The subscription runs on a background task until the returned handle is disposed,
    /// <paramref name="ct"/> is cancelled, or the client is disposed.
    /// </summary>
    /// <remarks>
    /// Exceptions from the stream or from <paramref name="onEvent"/> are not thrown to the caller;
    /// handle errors inside <paramref name="onEvent"/> or subscribe to <see cref="TaskScheduler.UnobservedTaskException"/>.
    /// </remarks>
    /// <returns>An <see cref="IDisposable"/> that stops the subscription when disposed.</returns>
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

    private sealed class SubscriptionHandle(CancellationTokenSource cts) : IDisposable
    {
        public void Dispose()
        {
            cts.Cancel();
            cts.Dispose();
        }
    }

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