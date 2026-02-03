namespace UmaDb.Csharp;

public class UmaQuery
{
    private List<UmaQueryItem> Items { get; } = [];
    public bool Backwards { get; private set; }
    public long? Start { get; private set; }
    public int? Limit { get; private set; }
    public bool Subscribe { get; private set; } 
    
    public static UmaQuery Where(string[]? types = null, string[]? tags = null)
    {
        var query = new UmaQuery();
        return query.Or(types, tags);
    }
    
    public UmaQuery Or(string[]? types = null, string[]? tags = null)
    {
        Items.Add(new UmaQueryItem 
        { 
            Types = types?.ToList() ?? [], 
            Tags = tags?.ToList() ?? [] 
        });
        return this;
    }
    
    public UmaQuery ReadBackwards(bool backwards = true)
    {
        Backwards = backwards;
        return this;
    }
    
    public UmaQuery FromPosition(long? start)
    {
        Start = start;
        return this;
    }
    
    public UmaQuery Take(int? limit)
    {
        Limit = limit;
        return this;
    }
    
    public UmaQuery SubscribeToUpdates(bool subscribe = true)
    {
        Subscribe = subscribe;
        return this;
    }
    
    internal UmaDb.Core.Query ToProto() => new()
    {
        Items = Items.Select(i => new UmaDb.Core.QueryItem
        {
            Types = i.Types,
            Tags = i.Tags
        }).ToList()
    };
}

public class UmaQueryItem
{
    public List<string> Types { get; init; } = [];
    public List<string> Tags { get; init; } = [];
}