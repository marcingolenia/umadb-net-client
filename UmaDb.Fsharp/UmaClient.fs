module UmaDb.Fsharp.Client

open System
open System.Threading
open Client.UmaConnection
open ProtoBuf.Grpc.Client
open UmaDb.Client
open UmaDb.Core
open ProtoBuf.Grpc
open Grpc.Core
open FSharp.Control
open Errors
open Types
open Extensions

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
    
let readWithOptions (query: QueryItem list)
                    (options: QueryOptions)
                    (client: UmaClient)
                    (ct: CancellationToken) =
    taskSeq {
        try
            ct.ThrowIfCancellationRequested()
            let batches = client.ReadBatches query options ct
            for batch in batches do
                ct.ThrowIfCancellationRequested()
                yield! Conversion.toSequencedUmaEventList batch
        with
        | :? RpcException as ex -> raise (UmaDbException.ToUmaDbException(ex))
    }
    
let read (query: QueryItem list) (client: UmaClient) = 
    readWithOptions query QueryOptions.defaults client CancellationToken.None

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
