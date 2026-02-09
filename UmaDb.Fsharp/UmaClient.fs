module UmaDb.Fsharp.Client

open System
open System.Collections.Generic
open System.Threading
open Client.UmaConnection
open ProtoBuf.Grpc.Client
open UmaDb.Client
open UmaDb.Core
open ProtoBuf.Grpc
open Grpc.Core
open Errors
open Types
open Extensions

type UmaClient(connection: UmaConnectionResult) =
    let service = connection.GetCallInvoker().CreateGrpcService<IDcbService>()
    member internal _.Service = service

    interface IDisposable with
        member _.Dispose() = (connection :> IDisposable).Dispose()

let private readBatches (service: IDcbService) (query: Query) (options: QueryOptions) (ct: CancellationToken) =
    let queryProto = Query.toProto query
    let request: ReadRequest =
        { Query = Option.defaultValue Nullable.query queryProto
          Start = toNullableUInt64 options.FromPosition
          Backwards = options.Backwards
          Limit = toNullableUInt32 options.Limit
          Subscribe = options.Subscribe
          BatchSize = toNullableUInt32 options.BatchSize }
    service.Read(request, CallContext.op_Implicit ct)

let readWithOptions (query: Query) (options: QueryOptions) (client: UmaClient): IAsyncEnumerable<SequencedUmaEvent> =
    let ct = CancellationToken.None
    let batches = readBatches client.Service query options ct
    
    { new IAsyncEnumerable<SequencedUmaEvent> with
        member _.GetAsyncEnumerator(cancellationToken) =
            let enumerator = batches.GetAsyncEnumerator(cancellationToken)
            let mutable currentEvents = ResizeArray<SequencedUmaEvent>()
            let mutable currentIndex = 0
            
            { new IAsyncEnumerator<SequencedUmaEvent> with
                member _.MoveNextAsync() =
                    task {
                        try
                            // If we have events in current batch, return next one
                            if currentIndex < currentEvents.Count then
                                currentIndex <- currentIndex + 1
                                return true
                            else
                                // Get next batch
                                let! hasMore = enumerator.MoveNextAsync()
                                if hasMore then
                                    let batch = enumerator.Current
                                    currentEvents <- ResizeArray(Conversion.toSequencedUmaEventList batch)
                                    currentIndex <- 1
                                    return currentEvents.Count > 0
                                else
                                    return false
                        with
                        | :? RpcException as ex -> return raise (UmaDbException.ToUmaDbException(ex))
                    }
                    |> System.Threading.Tasks.ValueTask<bool>

                member _.Current =
                    if currentIndex > 0 && currentIndex <= currentEvents.Count then
                        currentEvents[currentIndex - 1]
                    else
                        raise (InvalidOperationException("Enumerator not positioned on valid element"))

                member _.DisposeAsync() = enumerator.DisposeAsync() } }

/// Read events as a stream (IAsyncEnumerable).
let read (query: Query) (client: UmaClient): IAsyncEnumerable<SequencedUmaEvent> =
    let options = QueryOptions.defaults
    readWithOptions query options client

/// Get the head position (last event position).
let head (client: UmaClient): Async<int64 option> =
    async {
        try
            let! ct = Async.CancellationToken
            let! response = client.Service.Head({ _unused = Nullable() }, CallContext.op_Implicit ct).ToAsync()
            return
                if response.Position.HasValue then
                    Some(int64 (response.Position.GetValueOrDefault()))
                else
                    None
        with
        | :? RpcException as ex -> return raise (UmaDbException.ToUmaDbException(ex))
    }

/// Get tracking info for a source.
let trackingInfo (source: string) (client: UmaClient) (ct: CancellationToken): Async<int64 option> =
    async {
        try
            let! response = client.Service.GetTrackingInfo({ Source = source }, CallContext.op_Implicit ct).ToAsync()
            return
                if response.Position.HasValue then
                    Some(int64 (response.Position.GetValueOrDefault()))
                else
                    None
        with
        | :? RpcException as ex -> return raise (UmaDbException.ToUmaDbException(ex))
    }

/// Read all events matching the query and return as a list with head position.
let readAll (query: Query) (client: UmaClient): Async<SequencedUmaEvent list * int64 option> =
    async {
        let options = QueryOptions.defaults
        let! headPos = head client 
        let mutable events = ResizeArray<SequencedUmaEvent>()
        
        let stream = readWithOptions query options client
        let enumerator = stream.GetAsyncEnumerator(CancellationToken.None)
        
        try
            let mutable continueLoop = true
            while continueLoop do
                let! hasMore = enumerator.MoveNextAsync().ToAsync()
                if hasMore then
                    events.Add(enumerator.Current)
                else
                    continueLoop <- false
            
            return (List.ofSeq events, headPos)
        finally
            let disposeTask = enumerator.DisposeAsync()
            if disposeTask.IsCompletedSuccessfully then
                ()
            else
                let! _ = disposeTask.AsTask() |> Async.AwaitTask
                ()
    }

/// Subscribe to events matching the query (keeps stream open for new events).
let subscribe (query: Query) (client: UmaClient): IAsyncEnumerable<SequencedUmaEvent> =
    let options = { QueryOptions.defaults with Subscribe = true }
    readWithOptions query options client

// ===== Appending =====

/// Append operation builder for composable append operations.
type AppendOperation =
    { Client: UmaClient
      Events: UmaEvent list
      FailIfMatch: Query option
      After: int64 option
      TrackingInfo: UmaTrackingInfo option }

/// Start an append operation.
let append (events: UmaEvent list) (client: UmaClient): AppendOperation =
    { Client = client
      Events = events
      FailIfMatch = None
      After = None
      TrackingInfo = None }

/// Add fail-if-match condition to append operation.
let failIfMatch (query: Query) (op: AppendOperation): AppendOperation =
    { op with FailIfMatch = Some query }

/// Add after position condition to append operation.
let after (position: int64) (op: AppendOperation): AppendOperation =
    { op with After = Some position }

/// Add tracking info to append operation.
let track (source: string) (position: int64) (op: AppendOperation): AppendOperation =
    { op with TrackingInfo = Some { Source = source; Position = position } }

/// Execute the append operation.
let execute (op: AppendOperation) (ct: CancellationToken): Async<AppendResult> =
    async {
        if List.isEmpty op.Events then
            return IntegrityError "Events list cannot be empty"
        else
            try
                let condition =
                    op.FailIfMatch
                    |> Option.bind Query.toProto
                    |> Option.map (fun q ->
                        { FailIfEventsMatch = q
                          After = toNullableUInt64 op.After })
                    |> Option.defaultValue Nullable.appendCondition

                let trackingInfo' =
                    op.TrackingInfo
                    |> Option.map (fun info ->
                        { TrackingInfo.Source = info.Source
                          Position = uint64 info.Position })
                    |> Option.defaultValue Nullable.trackingInfo

                let request: AppendRequest =
                    { Events = op.Events |> List.map Conversion.fromUmaEvent |> ResizeArray
                      Condition = condition
                      TrackingInfo = trackingInfo' }

                let! response = op.Client.Service.Append(request, CallContext.op_Implicit ct).ToAsync()
                return Success(int64 response.Position)
            with
            | :? RpcException as ex ->
                let umaEx = UmaDbException.ToUmaDbException(ex)
                match umaEx with
                | :? IntegrityException -> return IntegrityError umaEx.Message
                | _ -> return raise umaEx
    }
