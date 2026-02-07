namespace UmaDb.Csharp.Messages;

internal record UmaReadBatch(
    IReadOnlyList<SequencedUmaEvent> Events,
    long? Head
);