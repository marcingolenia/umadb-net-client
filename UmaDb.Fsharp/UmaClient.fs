/// <summary>F# client for reading and appending events to UmaDB via gRPC (DCB-compliant).</summary>
/// <remarks>Create with <c>connect host port |> build</c> (or <c>withTls</c> / <c>withApiKey</c>). Reuse one instance per process; implement <c>IDisposable</c> and dispose when shutting down.</remarks>
module UmaDb.Fsharp.Client

open System
open System.Threading
open System.Threading.Tasks
open Client.UmaConnection
open ProtoBuf.Grpc.Client
open UmaDb.Core
open ProtoBuf.Grpc
open Grpc.Core
open FSharp.Control
open Errors
open Types

/// <summary>Client instance for a single UmaDB connection. Dispose when no longer needed.</summary>
type UmaClient(connection: UmaConnectionResult) =
    let service = connection.GetCallInvoker().CreateGrpcService<IDcbService>()
    member internal _.Service = service
    
    member internal _.ReadBatches (query: QueryItem list) (options: QueryOptions) (ct: CancellationToken) =
        let queryProto = Query.toProto query
        let request: ReadRequest =
            { Query = Option.defaultValue Nullable.query queryProto
              Start = toNullableUInt64 options.FromPosition
              Backwards = options.Backwards
              Limit = toNullableUInt32 options.Limit
              Subscribe = options.Subscribe
              BatchSize = toNullableUInt32 options.BatchSize }
        service.Read(request, CallContext.op_Implicit ct)

    interface IDisposable with
        member _.Dispose() = (connection :> IDisposable).Dispose()

/// <summary>Streams Sequenced Events matching the query (DCB read). Use for large result sets or incremental processing.</summary>
/// <param name="client">The UmaDB client.</param>
/// <param name="ct">Cancellation token.</param>
/// <param name="query">DCB Query: filter by Event Type and/or Tags (Query Items are OR'd).</param>
/// <param name="options">Read options (position, limit, batch size, backwards, subscribe). Use <c>QueryOptions.defaults</c> or pipe <c>QueryOptions.subscribe</c> for a live stream.</param>
/// <returns>TaskSeq of <c>SequencedUmaEvent</c> (batch boundaries are internal).</returns>
/// <remarks>See <see href="https://dcb.events/specification/">DCB Specification – Reading Events</see>.</remarks>
let readWithOptions (client: UmaClient)
                    (ct: CancellationToken)
                    (query: QueryItem list)
                    (options: QueryOptions)
                    : TaskSeq<SequencedUmaEvent> =
    taskSeq {
        try
            ct.ThrowIfCancellationRequested()
            let batches = client.ReadBatches query options ct
            for batch in batches do
                yield! Conversion.toSequencedUmaEventList batch
        with
        | :? RpcException as ex -> raise (UmaDbException.ToUmaDbException(ex))
    }
    
/// <summary>Reads all events matching the query and returns them plus the head position. Use for small result sets or when building a decision model.</summary>
/// <param name="client">The UmaDB client.</param>
/// <param name="query">DCB Query to filter by types and tags.</param>
/// <returns>Task of (events, head). When there are no events, head is <c>None</c> (omit <c>after</c> in append conditions per DCB). Use head as <c>withAfter</c> in conditional appends.</returns>
let readList (client: UmaClient) (query: QueryItem list): Task<SequencedUmaEvent list * int64 option> =
    task {
        let! events =
            readWithOptions client CancellationToken.None query QueryOptions.defaults 
            |> TaskSeq.toListAsync
        let head = events |> List.tryLast |> Option.map (fun e -> e.Position)
        return events, head
    }

/// <summary>Returns the Sequence Position of the last event in the log, or <c>None</c> if the log is empty (DCB Sequence Position).</summary>
/// <param name="client">The UmaDB client.</param>
/// <param name="ct">Cancellation token.</param>
/// <returns>Task of optional position.</returns>
let readHead (client: UmaClient) (ct: CancellationToken) =
    task {
        try
            let! response = client.Service.Head({ _unused = Nullable() }, CallContext.op_Implicit ct)
            return
                if response.Position.HasValue then
                    Some(int64 (response.Position.GetValueOrDefault()))
                else
                    None
        with
        | :? RpcException as ex -> return raise (UmaDbException.ToUmaDbException(ex))
    }

/// <summary>Returns the last recorded position for the given upstream source, or <c>None</c> if not found. Used for exactly-once ingestion from external streams.</summary>
/// <param name="client">The UmaDB client.</param>
/// <param name="ct">Cancellation token.</param>
/// <param name="source">Upstream source name (e.g. stream or topic).</param>
/// <returns>Task of optional position.</returns>
let readTrackingInfo (client: UmaClient) (ct: CancellationToken) (source: string): Task<int64 option> =
    task {
        try
            let! response = client.Service.GetTrackingInfo({ Source = source }, CallContext.op_Implicit ct)
            return
                if response.Position.HasValue then
                    Some(int64 (response.Position.GetValueOrDefault()))
                else
                    None
        with
        | :? RpcException as ex -> return raise (UmaDbException.ToUmaDbException(ex))
    }

/// <summary>Describes an append operation: events plus optional Append Condition and tracking (DCB append).</summary>
/// <remarks>Build with <c>appendOperation</c>, then pipe <c>failIfMatch</c>, <c>withAfter</c>, <c>track</c> as needed. Pass to <c>append</c>.</remarks>
type AppendOperation =
    { Events: UmaEvent list
      FailIfMatch: Query option
      After: int64 option
      TrackingInfo: UmaTrackingInfo option }

/// <summary>Starts building an append operation. Events are appended atomically (DCB: append MUST be atomic).</summary>
/// <param name="events">Events to append. Must not be empty.</param>
/// <returns>An <c>AppendOperation</c> with no condition; use <c>failIfMatch</c> / <c>withAfter</c> / <c>track</c> then <c>append</c>.</returns>
let appendOperation (events: UmaEvent list): AppendOperation =
    { Events = events
      FailIfMatch = None
      After = None
      TrackingInfo = None }

/// <summary>Adds an Append Condition: append fails if the store contains any event matching the query (DCB failIfEventsMatch).</summary>
/// <param name="query">Same query used when building the decision model; typically combined with <c>withAfter</c> for optimistic concurrency.</param>
/// <param name="op">The append operation to update.</param>
/// <returns>Updated operation. Use with <c>withAfter</c> so only events after that position are considered.</returns>
let failIfMatch (query: Query) (op: AppendOperation): AppendOperation =
    { op with FailIfMatch = Some query }

/// <summary>Sets the <c>after</c> Sequence Position for the Append Condition (DCB after). Events before this position are ignored when checking failIfEventsMatch.</summary>
/// <param name="position">Usually the head from <c>readList</c> or <c>readHead</c>. <c>None</c> omits the field (no events ignored; append fails if any event matches).</param>
/// <param name="op">The append operation to update.</param>
/// <returns>Updated operation.</returns>
let withAfter (position: int64 option) (op: AppendOperation): AppendOperation =
    match position with Some p -> { op with After = Some p } | None -> op

/// <summary>Adds upstream tracking info: position is stored atomically with the events. Positions must increase per source (exactly-once ingestion).</summary>
/// <param name="source">Upstream source name.</param>
/// <param name="position">Position to record for that source.</param>
/// <param name="op">The append operation to update.</param>
/// <returns>Updated operation.</returns>
let track (source: string) (position: int64) (op: AppendOperation): AppendOperation =
    { op with TrackingInfo = Some { Source = source; Position = position } }


/// <summary>Appends the events from the operation atomically. Optionally enforces the Append Condition and/or records tracking info.</summary>
/// <param name="client">The UmaDB client.</param>
/// <param name="ct">Cancellation token.</param>
/// <param name="op">Operation built with <c>appendOperation</c>, <c>failIfMatch</c>, <c>withAfter</c>, <c>track</c>.</param>
/// <returns>Task of <c>Ok position</c> (commit position of last appended event) or <c>Error (ErrorMessage _)</c> when the append condition fails or tracking position is not strictly increasing.</returns>
let append (client: UmaClient) (ct: CancellationToken) (op: AppendOperation) : Task<Result<int64, IntegrityError>> =
    task {
        let condition =
            match op.FailIfMatch |> Option.bind Query.toProto with
            | None when Option.isNone op.After -> Nullable.appendCondition
            | q ->
                { FailIfEventsMatch = Option.defaultValue Nullable.query q
                  After = toNullableUInt64 op.After }

        let trackingInfo =
            match op.TrackingInfo with
            | None -> Nullable.trackingInfo
            | Some info -> { 
                TrackingInfo.Source = info.Source; 
                Position = uint64 info.Position }

        let request: AppendRequest =
            { Events = op.Events |> List.map Conversion.fromUmaEvent |> ResizeArray
              Condition = condition
              TrackingInfo = trackingInfo }

        try
            let! response = client.Service.Append(request, CallContext.op_Implicit ct)
            return Ok(int64 response.Position)
        with
        | :? RpcException as ex ->
            let umaEx = UmaDbException.ToUmaDbException(ex)
            match umaEx with
            | :? IntegrityException -> return Error (ErrorMessage umaEx.Message)
            | _ -> return raise umaEx
    }


/// <summary>Handle for an active subscription. Dispose to stop the subscription.</summary>
type SubscriptionHandle(cts: CancellationTokenSource) =
    interface IDisposable with
        member _.Dispose() = cts.Cancel(); cts.Dispose()

/// <summary>Streams events matching the query as they become available (subscription), invoking the async callback for each event sequentially. Use for projections that need async work (e.g. DB writes).</summary>
/// <param name="client">The UmaDB client.</param>
/// <param name="ct">When cancelled, the subscription stops.</param>
/// <param name="query">DCB Query to filter by types and tags.</param>
/// <param name="onEvent">Async callback for each Sequenced Event. Should be idempotent when building projections. Receives the linked cancellation token.</param>
/// <returns>Disposable that stops the subscription when disposed (e.g. <c>use _ = subscribeWithCallback ...</c>). Exceptions in the stream or in <c>onEvent</c> are not thrown to the caller—handle them inside <c>onEvent</c>.</returns>
let subscribeWithCallback (client: UmaClient) (ct: CancellationToken) (query: QueryItem list) (onEvent: SequencedUmaEvent * CancellationToken -> Task) : IDisposable =
    let stopCts = new CancellationTokenSource()
    let linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, stopCts.Token)
    let token = linkedCts.Token
    let options = QueryOptions.defaults |> QueryOptions.subscribe

    Task.Run((fun () ->
        task {
            try
                for evt in readWithOptions client token query options do
                    do! onEvent(evt, token)
            finally
                linkedCts.Dispose()
        } :> Task), token)
    |> ignore

    new SubscriptionHandle(stopCts) :> IDisposable