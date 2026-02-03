namespace UmaDb.Csharp.Messages;

public record UmaReadBatch(
    IReadOnlyList<SequencedUmaEvent> Events, 
    long? Head
);