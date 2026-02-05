namespace UmaDb.Csharp.Messages;

/// <summary>
/// One batch of events from a streamed read. Each batch may also report the last known head position.
/// </summary>
/// <param name="Events">Events in this batch.</param>
/// <param name="Head">Last known sequence position after this batch (for use as <c>after</c> in conditional appends).</param>
public record UmaReadBatch(
    IReadOnlyList<SequencedUmaEvent> Events,
    long? Head
);