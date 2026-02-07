namespace UmaDb.Client.Messages;

internal record UmaReadBatch(
    IReadOnlyList<SequencedUmaEvent> Events,
    long? Head
);