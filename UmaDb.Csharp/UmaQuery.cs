using UmaDb.Core;

namespace UmaDb.Csharp;

public class UmaFilter
{
    internal UmaFilter()
    {
    }

    internal List<QueryItem> Items { get; } = [];

    public static UmaFilter All => new();

    public static UmaFilter Where(string[]? types = null, string[]? tags = null) => 
        new UmaFilter().Or(types, tags);

    public UmaFilter Or(string[]? types = null, string[]? tags = null)
    {
        Items.Add(new QueryItem
        {
            Types = types?.ToList() ?? [],
            Tags = tags?.ToList() ?? []
        });
        return this;
    }


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

public class UmaQueryOptions
{
    public long? FromPosition { get; set; }
    public int? Limit { get; set; }
    public int? BatchSize { get; set; }
    public bool Backwards { get; set; }
    public bool Subscribe { get; set; }
}

public class UmaQuery(UmaFilter filter, UmaQueryOptions options)
{
    public UmaFilter Filter { get; } = filter;
    public UmaQueryOptions Options { get; } = options;
}