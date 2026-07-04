/// <summary>Query and read options for filtering and streaming events (DCB-compliant). Build queries as <c>QueryItem list</c>; use <c>QueryOptions</c> for position, limit, batch size, etc.</summary>
module UmaDb.Client.Query

open UmaDb.Core

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
      Backwards: bool }

let defaultQueryOptions =
    { FromPosition = None
      Limit = None
      BatchSize = None
      Backwards = false }

module internal Query =
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

    /// Set batch size.
    let batchSize (size: int) (options: QueryOptions): QueryOptions =
        { options with BatchSize = Some size }