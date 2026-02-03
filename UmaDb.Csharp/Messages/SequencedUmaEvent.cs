namespace UmaDb.Csharp.Messages;

/// <summary>
/// An event retrieved from UmaDB with its server-assigned position.
/// </summary>
public record SequencedUmaEvent(
    long Position,
    UmaEvent Event
);