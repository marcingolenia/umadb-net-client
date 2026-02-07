using UmaDb.Core;

namespace UmaDb.Client;

/// <summary>
/// Builds a filter over the event log by event type and tags.
/// Use with <see cref="UmaClient.ReadAsync"/>, <see cref="UmaClient.ReadListAsync"/>, <see cref="UmaClient.SubscribeAsync"/>, <see cref="UmaClient.SubscribeWithCallback"/>, and append conditions.
/// </summary>
public class UmaQuery
{
    internal UmaQuery()
    {
    }

    /// <summary>Stored as (types, tags) to avoid allocating List until ToProto (when query is actually used).</summary>
    internal List<(string[]? Types, string[]? Tags)> Items { get; } = [];

    /// <summary>Filter that matches all events (no type or tag restriction).</summary>
    public static UmaQuery All => new();

    /// <summary>
    /// Starts a filter: events match if type is in <paramref name="types"/> (or any if null) and tags include all of <paramref name="tags"/> (or any if null).
    /// Chain with <see cref="Or"/> to add alternative criteria (OR).
    /// </summary>
    public static UmaQuery Where(string[]? types = null, string[]? tags = null) =>
        new UmaQuery().Or(types, tags);

    /// <summary>
    /// Adds another query item (OR). An event matches the filter if it matches this item or any previous item.
    /// </summary>
    public UmaQuery Or(string[]? types = null, string[]? tags = null)
    {
        Items.Add((types, tags));
        return this;
    }

    /// <summary>
    /// Configures read options (position, limit, batch size, direction, subscribe) and returns a <see cref="UmaQueryWithOptions"/> for use with read/subscribe APIs.
    /// </summary>
    public UmaQueryWithOptions WithOptions(Action<UmaQueryOptions>? configure = null)
    {
        var options = new UmaQueryOptions();
        configure?.Invoke(options);
        return new UmaQueryWithOptions(this, options);
    }

    internal Query? ToProto()
    {
        if (Items.Count == 0)
            return null;
        var protoItems = new List<QueryItem>(Items.Count);
        for (var i = 0; i < Items.Count; i++)
        {
            var (types, tags) = Items[i];
            protoItems.Add(new QueryItem
            {
                Types = types is { Length: > 0 } ? new List<string>(types) : [],
                Tags = tags is { Length: > 0 } ? new List<string>(tags) : []
            });
        }
        return new Query { Items = protoItems };
    }
}

/// <summary>
/// Options for a read or subscribe operation: start position, limit, batch size, direction, and whether to keep the stream open for new events.
/// </summary>
public class UmaQueryOptions
{
    /// <summary>Read only events at or after this sequence position (or at or before if <see cref="Backwards"/> is true).</summary>
    public long? FromPosition { get; set; }
    /// <summary>Maximum number of events to return.</summary>
    public int? Limit { get; set; }
    /// <summary>Hint for how many events to return per batch when streaming.</summary>
    public int? BatchSize { get; set; }
    /// <summary>If true, read from the end of the log (or from <see cref="FromPosition"/>) backwards.</summary>
    public bool Backwards { get; set; }
    /// <summary>If true, keep the stream open and deliver new events as they are appended (subscription).</summary>
    public bool Subscribe { get; set; }
}

/// <summary>
/// A read request: a query plus options. Create via <see cref="UmaQuery.WithOptions"/>.
/// </summary>
/// <param name="query">DCB Query to filter by types and tags.</param>
/// <param name="options">Read options (position, limit, batch size, backwards, subscribe).</param>
public class UmaQueryWithOptions(UmaQuery query, UmaQueryOptions options)
{
    /// <summary>The DCB query used for this read.</summary>
    public UmaQuery Query { get; } = query;
    /// <summary>The read options.</summary>
    public UmaQueryOptions Options { get; } = options;
}