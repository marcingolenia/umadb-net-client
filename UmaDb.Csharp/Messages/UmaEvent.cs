namespace UmaDb.Csharp.Messages;

public record UmaEvent(
    string EventType,
    ReadOnlyMemory<byte> Data,
    string[]? Tags = null,
    Guid? Id = null
);