module UmaDb.Fsharp.Client

open System
open System.Threading
open System.Threading.Tasks
open Client.UmaConnection
open ProtoBuf.Grpc.Client
open UmaDb.Client
open UmaDb.Core
open ProtoBuf.Grpc
open Grpc.Core
open FSharp.Control
open Errors
open Types

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
                yield! Conversion.toSequencedUmaEventList batch
        with
        | :? RpcException as ex -> raise (UmaDbException.ToUmaDbException(ex))
    }
    
/// Returns (events, position of last event). When there are no events, position is None (omit <c>after</c> per DCB).
let readList (client: UmaClient) (query: QueryItem list): Task<SequencedUmaEvent list * int64 option> =
    task {
        let! events =
            readWithOptions query QueryOptions.defaults client CancellationToken.None
            |> TaskSeq.toListAsync
        let head = events |> List.tryLast |> Option.map (fun e -> e.Position)
        return events, head
    }

let readHead (ct: CancellationToken) (client: UmaClient)  =
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

let trackingInfo (source: string) (client: UmaClient) (ct: CancellationToken): Task<int64 option> =
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

type AppendOperation =
    { Events: UmaEvent list
      FailIfMatch: Query option
      After: int64 option
      TrackingInfo: UmaTrackingInfo option }

let appendOperation (events: UmaEvent list): AppendOperation =
    { Events = events
      FailIfMatch = None
      After = None
      TrackingInfo = None }

/// Add fail-if-match condition to append operation.
let failIfMatch (query: Query) (op: AppendOperation): AppendOperation =
    { op with FailIfMatch = Some query }

/// Add after-position from read when present; when None, leaves condition omitted (DCB: omit = no events ignored).
let withAfter (position: int64 option) (op: AppendOperation): AppendOperation =
    match position with Some p -> { op with After = Some p } | None -> op

/// Add tracking info to append operation.
let track (source: string) (position: int64) (op: AppendOperation): AppendOperation =
    { op with TrackingInfo = Some { Source = source; Position = position } }


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
