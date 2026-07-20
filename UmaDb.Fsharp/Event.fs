/// <summary>Event and tracking types for UmaDB (DCB-compliant).</summary>
module UmaDb.Client.Event

open System
open UmaDb.Core

/// <summary>Single event to append or as read from the log. <c>Id</c> can be set for idempotent appends.</summary>
type UmaEvent =
    { EventType: string
      Data: ReadOnlyMemory<byte>
      Tags: string list option
      Metadata: (string * string) list option
      Id: Guid option }

/// <summary>Event together with its sequence position in the log (DCB read result).</summary>
type SequencedUmaEvent =
    { Position: int64
      Event: UmaEvent }

/// <summary>Upstream source and position for exactly-once ingestion (stored atomically with appended events).</summary>
type UmaTrackingInfo =
    { Source: string
      Position: int64 }

module internal Conversion =
    let toUmaEvent (e: Event): UmaEvent =
        { EventType = e.EventType
          Data = ReadOnlyMemory(e.Data)
          Tags =
              if e.Tags = null || e.Tags.Count = 0 then
                  None
              else
                  Some(List.ofSeq e.Tags)
          Metadata =
              if e.Metadata = null || e.Metadata.Count = 0 then
                  None
              else
                  Some(e.Metadata |> Seq.map (fun m -> m.Key, m.Value) |> List.ofSeq)
          Id =
              if String.IsNullOrEmpty e.Uuid then
                  None
              else
                  match Guid.TryParse e.Uuid with
                  | true, guid -> Some guid
                  | false, _ -> None }

    let toSequencedUmaEvent (e: SequencedEvent): SequencedUmaEvent =
        { Position = int64 e.Position
          Event = toUmaEvent e.Event }

    let fromUmaEvent (e: UmaEvent): Event =
        { EventType = e.EventType
          Tags =
              match e.Tags with
              | Some tags -> ResizeArray(tags)
              | None -> ResizeArray()
          Data =
              if e.Data.IsEmpty then
                  Array.empty
              else
                  e.Data.ToArray()
          Uuid =
              match e.Id with
              | Some id -> id.ToString()
              | None -> Guid.NewGuid().ToString()
          Metadata =
              match e.Metadata with
              | Some metadata -> ResizeArray(metadata |> List.map (fun (k, v) -> { Key = k; Value = v }))
              | None -> ResizeArray()
        }

    let private eventsToList (events: ResizeArray<SequencedEvent>): SequencedUmaEvent list =
        if events = null || events.Count = 0 then
            []
        else
            events
            |> Seq.map toSequencedUmaEvent
            |> List.ofSeq

    let toSequencedUmaEventList (response: ReadResponse): SequencedUmaEvent list =
        eventsToList response.Events

    let toSubscribeUmaEventList (response: SubscribeResponse): SequencedUmaEvent list =
        eventsToList response.Events


let inline toNullableUInt64 (value: int64 option) =
    match value with
    | Some v -> Nullable(uint64 v)
    | None -> Nullable()


let inline toNullableUInt32 (value: int option) =
    match value with
    | Some v -> Nullable(uint32 v)
    | None -> Nullable()