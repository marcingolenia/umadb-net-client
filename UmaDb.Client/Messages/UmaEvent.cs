namespace UmaDb.Client.Messages;

/// <summary>
/// A single event to append or that was read from the log.
/// </summary>
/// <param name="EventType">Logical type (e.g. "OrderCreated"). Used for filtering in queries.</param>
/// <param name="Data">Opaque payload, typically JSON-serialized.</param>
/// <param name="Tags">Optional tags for filtering (e.g. aggregate or stream id). All listed tags must match in a query item.</param>
/// <param name="Metadata">Optional key-value metadata (e.g. correlation id, source). Keys are unique — the server rejects duplicate keys on append.</param>
/// <param name="Id">Optional unique id. When set, conditional appends with the same id are idempotent (same commit position).</param>
public record UmaEvent(
    string EventType,
    ReadOnlyMemory<byte> Data,
    IReadOnlyList<string>? Tags = null,
    IReadOnlyDictionary<string, string>? Metadata = null,
    Guid? Id = null
);