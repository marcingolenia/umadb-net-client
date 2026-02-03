using Client;
using Microsoft.FSharp.Core;
using ProtoBuf.Grpc.Client;
using UmaDb.Core;
using UmaDb.Csharp.Messages;

namespace UmaDb.Csharp;

/// <summary>
///     Client for reading and appending events to an UmaDB server over its gRPC API.
///     Use <see cref="Connect" /> to create a connection; then call <see cref="AppendAsync" /> to write events
///     and <see cref="ReadAsync" /> or <see cref="ReadListAsync" /> to read them.
/// </summary>
public sealed class UmaClient(UmaConnection.UmaConnectionResult connection) : IDisposable
{
    private readonly UmaConnection.UmaConnectionResult _connection =
        connection ?? throw new ArgumentNullException(nameof(connection));

    private readonly IDcbService _service = connection.GetCallInvoker().CreateGrpcService<IDcbService>();

    /// <summary>
    ///     Releases the connection to the UmaDB server. Call when the client is no longer needed.
    /// </summary>
    public void Dispose()
    {
        ((IDisposable)_connection).Dispose();
    }

    /// <summary>
    ///     Returns the position of the last event recorded in the store.
    /// </summary>
    /// <param name="ct">Optional cancellation token.</param>
    /// <returns>
    ///     The sequence number  of the last recorded event, or null if no events have been recorded yet.
    /// </returns>
    /// <remarks>
    ///     Useful for counting recorded events or for determining where to start when subscribing only to new events.
    /// </remarks>
    public async ValueTask<long?> GetHeadAsync(CancellationToken ct = default)
    {
         var response = await _service.Head(new HeadRequest { _unused = null }, ct);
         return (long?)(response.Position ?? null);
    }

    /// <summary>
    ///     Reads events from the store and collects them into a single list.
    /// </summary>
    /// <param name="query">
    ///     The read request (query, start, limit, etc.). Subscription is not used; the stream is read to
    ///     completion.
    /// </param>
    /// <param name="ct">Optional cancellation token.</param>
    /// <returns>A task that completes with a list of <see cref="SequencedEvent" /> instances in order.</returns>
    /// <remarks>
    ///     Convenient when you need all matching events in memory (e.g. for building a decision model from a bounded stream).
    ///     For large or unbounded streams, prefer <see cref="ReadAsync" /> and consume the stream incrementally.
    /// </remarks>
    public async Task<List<SequencedUmaEvent>> ReadListAsync(UmaQuery query, CancellationToken ct = default)
    {
        var results = new List<SequencedUmaEvent>();
        await foreach (var response in ReadAsync(query, ct).ConfigureAwait(false))
        {
            results.AddRange(response.Events);
        }

        return results;
    }

    /// <summary>
    ///     Overload to Read using the fluent UmaQuery directly.
    ///     The query can include filter criteria (via <see cref="UmaQuery.Where" /> and <see cref="UmaQuery.Or" />)
    ///     as well as read options (via <see cref="UmaQuery.ReadBackwards" />, <see cref="UmaQuery.FromPosition" />,
    ///     <see cref="UmaQuery.Take" />, and <see cref="UmaQuery.SubscribeToUpdates" />).
    /// </summary>
    public IAsyncEnumerable<UmaReadBatch> ReadAsync(
        UmaQuery query,
        CancellationToken ct = default)
    {
        return _service.Read(new ReadRequest
        {
            Query = query.ToProto(),
            Start =
                query.Start.HasValue ? FSharpOption<ulong>.Some((ulong)query.Start.Value) : FSharpOption<ulong>.None,
            Backwards = query.Backwards,
            Limit = query.Limit.HasValue ? FSharpOption<uint>.Some((uint)query.Limit.Value) : FSharpOption<uint>.None,
            Subscribe = query.Subscribe
        }, ct).Select(response => new UmaReadBatch(
            (response.Events ?? []).Select(e => new SequencedUmaEvent(
                (long)e.Position,
                new UmaEvent(
                    e.Event.EventType,
                    e.Event.Data.ToArray(),
                    e.Event.Tags?.ToArray(),
                    string.IsNullOrEmpty(e.Event.Uuid) ? null : Guid.TryParse(e.Event.Uuid, out var guid) ? guid : null))).ToList(),
            response.Head.HasValue ? (long)response.Head.Value : null));
    }


    /// <summary>
    ///     Append using a fluent UmaQuery as a condition.
    /// </summary>
    public ValueTask<AppendResponse> AppendAsync(
        IEnumerable<UmaEvent> events,
        UmaQuery? failIfMatch = null,
        long? after = null,
        CancellationToken ct = default)
    {
        var request = new AppendRequest
        {
            Events = events.Select(e => new Event
            {
                EventType = e.EventType,
                Tags = e.Tags?.ToList() ?? [],
                Data = e.Data.ToArray(),
                Uuid = (e.Id ?? Guid.NewGuid()).ToString()
            }).ToList(),
            Condition = failIfMatch != null
                ? FSharpOption<AppendCondition>.Some(new AppendCondition
                {
                    FailIfEventsMatch = failIfMatch.ToProto(),
                    After = after.HasValue ? (ulong?)after.Value : null
                })
                : FSharpOption<AppendCondition>.None
        };

        return _service.Append(request, ct);
    }

    /// <summary>
    ///     Creates a client connected to an UmaDB server.
    /// </summary>
    /// <param name="host">Server host (e.g. "localhost" or "example.com").</param>
    /// <param name="port">Server port (1–65535).</param>
    /// <param name="caCert">
    ///     Optional path to a PEM-encoded root/CA certificate for TLS. Use for secure connections or when the
    ///     server uses a self-signed certificate. Required when <paramref name="apiKey" /> is set.
    /// </param>
    /// <param name="apiKey">
    ///     Optional API key for authentication. When set, TLS must be used (provide
    ///     <paramref name="caCert" />).
    /// </param>
    /// <returns>A connected <see cref="UmaClient" /> instance. Dispose it when done.</returns>
    /// <remarks>
    ///     Without <paramref name="caCert" />, an insecure (non-TLS) channel is used. For production or when using an API key,
    ///     pass a CA certificate path to use a secure channel.
    /// </remarks>
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