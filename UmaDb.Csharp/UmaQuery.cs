using UmaDb.Core;

namespace UmaDb.Csharp;

/// <summary>
/// Builds a filter over the event log by event type and tags.
/// Use with <see cref="UmaClient.ReadAsync"/>, <see cref="UmaClient.ReadListAsync"/>, <see cref="UmaClient.Subscribe"/>, and append conditions.
/// </summary>
public class UmaFilter
{
    internal UmaFilter()
    {
    }

    internal List<QueryItem> Items { get; } = [];

    /// <summary>Filter that matches all events (no type or tag restriction).</summary>
    public static UmaFilter All => new();

    /// <summary>
    /// Starts a filter: events match if type is in <paramref name="types"/> (or any if null) and tags include all of <paramref name="tags"/> (or any if null).
    /// Chain with <see cref="Or"/> to add alternative criteria (OR).
    /// </summary>
    public static UmaFilter Where(string[]? types = null, string[]? tags = null) =>
        new UmaFilter().Or(types, tags);

    /// <summary>
    /// Adds another query item (OR). An event matches the filter if it matches this item or any previous item.
    /// </summary>
    public UmaFilter Or(string[]? types = null, string[]? tags = null)
    {
        Items.Add(new QueryItem
        {
            Types = types?.ToList() ?? [],
            Tags = tags?.ToList() ?? []
        });
        return this;
    }

    /// <summary>
    /// Configures read options (position, limit, batch size, direction, subscribe) and returns a <see cref="UmaQuery"/> for use with read/subscribe APIs.
    /// </summary>
    public UmaQuery WithOptions(Action<UmaQueryOptions>? configure = null)
    {
        var options = new UmaQueryOptions();
        configure?.Invoke(options);
        return new UmaQuery(this, options);
    }

    internal Query? ToProto()
    {
        return Items.Count == 0 ? null : new Query { Items = Items };
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
/// A read request: a filter plus options. Create via <see cref="UmaFilter.WithOptions"/>.
/// </summary>
/// <param name="filter">The filter (types/tags) to apply.</param>
/// <param name="options">Read options (position, limit, batch size, backwards, subscribe).</param>
public class UmaQuery(UmaFilter filter, UmaQueryOptions options)
{
    /// <summary>The filter used for this read.</summary>
    public UmaFilter Filter { get; } = filter;
    /// <summary>The read options.</summary>
    public UmaQueryOptions Options { get; } = options;
}