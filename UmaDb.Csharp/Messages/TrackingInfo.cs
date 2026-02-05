namespace UmaDb.Csharp.Messages;

/// <summary>
/// Records that an upstream event source has been processed up to a given position.
/// Stored atomically with appended events; use for exactly-once processing. Positions must increase per source.
/// </summary>
/// <param name="Source">Name of the upstream stream or source.</param>
/// <param name="Position">Sequence position that has been processed.</param>
public record UmaTrackingInfo(string Source, long Position);