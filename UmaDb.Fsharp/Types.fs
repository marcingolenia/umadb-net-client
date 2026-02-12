module UmaDb.Fsharp.Types

open System
open UmaDb.Core

type UmaEvent =
    { EventType: string
      Data: ReadOnlyMemory<byte>
      Tags: string list option
      Id: Guid option }

type SequencedUmaEvent =
    { Position: int64
      Event: UmaEvent }

type UmaTrackingInfo =
    { Source: string
      Position: int64 }

/// QueryItem represents a single query constraint (DCB spec).
/// Types are OR'd together, Tags are AND'd together.
type QueryItem =
    { Types: string list
      Tags: string list }

/// Query is a list of QueryItems that are OR'd together (DCB spec).
/// Empty list means "match all events".
type Query = QueryItem list

/// QueryOptions for reading events (DCB spec).
type QueryOptions =
    { FromPosition: int64 option
      Limit: int option
      BatchSize: int option
      Backwards: bool
      Subscribe: bool }

let defaultQueryOptions =
    { FromPosition = None
      Limit = None
      BatchSize = None
      Backwards = false
      Subscribe = false }


module Query =
    let toProto (query: Query): UmaDb.Core.Query option =
        if List.isEmpty query then
            None
        else
            let items = 
                query
                |> List.map (fun item ->
                    { UmaDb.Core.QueryItem.Types = ResizeArray(item.Types)
                      Tags = ResizeArray(item.Tags) })
                |> ResizeArray
            let queryProto: UmaDb.Core.Query = { Items = items }
            Some queryProto

/// QueryOptions module for building query options.
module QueryOptions =
    /// Default query options.
    let defaults: QueryOptions = defaultQueryOptions

    /// Set the starting position.
    let fromPosition (position: int64) (options: QueryOptions): QueryOptions =
        { options with FromPosition = Some position }

    /// Set backwards reading.
    let backwards (options: QueryOptions): QueryOptions =
        { options with Backwards = true }

    /// Set limit on number of events.
    let limit (count: int) (options: QueryOptions): QueryOptions =
        { options with Limit = Some count }

    /// Enable subscription mode.
    let subscribe (options: QueryOptions): QueryOptions =
        { options with Subscribe = true }

    /// Set batch size.
    let batchSize (size: int) (options: QueryOptions): QueryOptions =
        { options with BatchSize = Some size }

module internal Conversion =
    let toUmaEvent (e: Event): UmaEvent =
        { EventType = e.EventType
          Data = ReadOnlyMemory(e.Data)
          Tags =
              if e.Tags = null || e.Tags.Count = 0 then
                  None
              else
                  Some(List.ofSeq e.Tags)
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
              | None -> Guid.NewGuid().ToString() }

    let toSequencedUmaEventList (response: ReadResponse): SequencedUmaEvent list =
        if response.Events = null || response.Events.Count = 0 then
            []
        else
            response.Events
            |> Seq.map toSequencedUmaEvent
            |> List.ofSeq

let inline toNullableUInt64 (value: int64 option) =
    match value with
    | Some v -> Nullable(uint64 v)
    | None -> Nullable()

let inline toNullableUInt32 (value: int option) =
    match value with
    | Some v -> Nullable(uint32 v)
    | None -> Nullable()