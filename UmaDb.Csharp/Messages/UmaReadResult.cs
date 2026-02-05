namespace UmaDb.Csharp.Messages;

/// <summary>
/// Result of a non-streaming read: all matching events and the head position after the read.
/// </summary>
public record UmaReadResult(
    IReadOnlyList<SequencedUmaEvent> Events,
    long? Head
);
