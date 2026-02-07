namespace UmaDb.Client.Messages;

/// <summary>
/// An event read from the log with its server-assigned sequence position.
/// </summary>
/// <param name="Position">Sequence number assigned by the server when the event was appended.</param>
/// <param name="Event">The event payload, type, and tags.</param>
public record SequencedUmaEvent(
    long Position,
    UmaEvent Event
);