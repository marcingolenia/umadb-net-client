module UmaDb

open System
open System.Collections.Generic
open System.Runtime.InteropServices
open System.Threading
open System.Threading.Tasks
open System.Runtime.CompilerServices
open Client.UmaConnection
open ProtoBuf.Grpc.Client
open UmaDb.Core
open ProtoBuf.Grpc
open Grpc.Core
open Errors

type UmaEvent =
    { EventType: string
      Data: ReadOnlyMemory<byte>
      Tags: string list option
      Id: Guid option }

type SequencedUmaEvent =
    { Position: int64
      Event: UmaEvent }

type UmaReadBatch =
    { Events: SequencedUmaEvent list
      Head: int64 option }

type UmaTrackingInfo =
    { Source: string
      Position: int64 }

type FilterItem =
    { Types: string list
      Tags: string list }

type UmaFilter = FilterItem list

type ReadOptions =
    { FromPosition: int64 option
      Limit: int option
      BatchSize: int option
      Backwards: bool
      Subscribe: bool }

let defaultReadOptions =
    { FromPosition = None
      Limit = None
      BatchSize = None
      Backwards = false
      Subscribe = false }


module Filter =
    let all: UmaFilter = []

    let where (types: string list option) (tags: string list option): FilterItem =
        { Types = defaultArg types []
          Tags = defaultArg tags [] }

    let or' (item: FilterItem) (filter: UmaFilter): UmaFilter = item :: filter

    let toProto (filter: UmaFilter): Query option =
        if List.isEmpty filter then
            None
        else
            filter
            |> List.map (fun item ->
                { QueryItem.Types = ResizeArray(item.Types)
                  Tags = ResizeArray(item.Tags) })
            |> ResizeArray
            |> fun items -> Some { Query.Items = items }

module Conversion =
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

    let toUmaReadBatch (response: ReadResponse): UmaReadBatch =
        { Events =
              if response.Events = null then
                  []
              else
                  response.Events |> Seq.map toSequencedUmaEvent |> List.ofSeq
          Head =
              if response.Head.HasValue then
                  Some(int64 response.Head.Value)
              else
                  None }

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

let inline toNullableUInt64 (value: int64 option) =
    match value with
    | Some v -> Nullable(uint64 v)
    | None -> Nullable()

let inline toNullableUInt32 (value: int option) =
    match value with
    | Some v -> Nullable(uint32 v)
    | None -> Nullable()

type UmaClient(connection: UmaConnectionResult) =
    let service = connection.GetCallInvoker().CreateGrpcService<IDcbService>()

    interface IDisposable with
        member _.Dispose() = (connection :> IDisposable).Dispose()

    member this.GetHeadAsync([<Optional>] ?ct: CancellationToken) : Async<int64 option> =
        async {
            let ct = defaultArg ct CancellationToken.None
            try
                let! response = service.Head({ _unused = Nullable() }, CallContext.op_Implicit ct).AsTask() |> Async.AwaitTask
                return
                    if response.Position.HasValue then
                        Some(int64 response.Position.Value)
                    else
                        None
            with
            | :? RpcException as ex -> return raise (UmaDbException.ToUmaDbException(ex))
        }

    member this.GetTrackingInfoAsync(source: string, [<Optional>] ?ct: CancellationToken) : Async<int64 option> =
        async {
            let ct = defaultArg ct CancellationToken.None
            try
                let! response = service.GetTrackingInfo({ Source = source }, CallContext.op_Implicit ct).AsTask() |> Async.AwaitTask
                return
                    if response.Position.HasValue then
                        Some(int64 response.Position.Value)
                    else
                        None
            with
            | :? RpcException as ex -> return raise (UmaDbException.ToUmaDbException(ex))
        }

    member this.ReadListAsync(filter: UmaFilter, [<Optional>] ?ct: CancellationToken) : Async<SequencedUmaEvent list * int64 option> =
        this.ReadListAsync(filter, defaultReadOptions, ?ct = ct)

    member this.ReadListAsync(filter: UmaFilter, options: ReadOptions, [<Optional>] ?ct: CancellationToken) : Async<SequencedUmaEvent list * int64 option> =
        async {
            let ct = defaultArg ct CancellationToken.None
            let results = ResizeArray<SequencedUmaEvent>()
            let mutable head = None

            try
                let enumerable = this.ReadAsync(filter, options, ct)
                let enumerator = enumerable.GetAsyncEnumerator(ct)
                try
                    let mutable hasMore = true
                    while hasMore do
                        let! hasValue = enumerator.MoveNextAsync().AsTask() |> Async.AwaitTask
                        if hasValue then
                            let batch = enumerator.Current
                            results.AddRange(batch.Events)
                            head <- batch.Head |> Option.orElse head
                        else
                            hasMore <- false
                finally
                    (enumerator :> IAsyncDisposable).DisposeAsync().AsTask() |> Async.AwaitTask |> ignore
            with
            | :? RpcException as ex -> return raise (UmaDbException.ToUmaDbException(ex))

            return (List.ofSeq results, head)
        }

    member this.ReadAsync(filter: UmaFilter, [<Optional>] ?ct: CancellationToken) : IAsyncEnumerable<UmaReadBatch> =
        this.ReadAsync(filter, defaultReadOptions, ?ct = ct)

    member this.ReadAsync(filter: UmaFilter, options: ReadOptions, [<Optional; EnumeratorCancellation>] ?ct: CancellationToken) : IAsyncEnumerable<UmaReadBatch> =
        let ct = defaultArg ct CancellationToken.None
        let query = Filter.toProto filter

        let request: ReadRequest =
            { Query = Option.defaultValue Nullable.query query
              Start = toNullableUInt64 options.FromPosition
              Backwards = options.Backwards
              Limit = toNullableUInt32 options.Limit
              Subscribe = options.Subscribe
              BatchSize = toNullableUInt32 options.BatchSize }

        let enumerable = service.Read(request, CallContext.op_Implicit ct)
        let mutable currentBatch = Unchecked.defaultof<UmaReadBatch>

        { new IAsyncEnumerable<UmaReadBatch> with
            member _.GetAsyncEnumerator(cancellationToken) =
                let enumerator = enumerable.GetAsyncEnumerator(cancellationToken)
                { new IAsyncEnumerator<UmaReadBatch> with
                    member _.MoveNextAsync() =
                        task {
                            try
                                let! hasValue = enumerator.MoveNextAsync()
                                if hasValue then
                                    currentBatch <- Conversion.toUmaReadBatch enumerator.Current
                                    return true
                                else
                                    return false
                            with
                            | :? RpcException as ex -> return raise (UmaDbException.ToUmaDbException(ex))
                        }
                        |> ValueTask<bool>

                    member _.Current = currentBatch

                    member _.DisposeAsync() = enumerator.DisposeAsync() } }

    member this.Subscribe(filter: UmaFilter, onEvent: SequencedUmaEvent -> unit, [<Optional>] ?ct: CancellationToken) : IDisposable =
        let ct = defaultArg ct CancellationToken.None
        let stopCts = new CancellationTokenSource()
        let linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, stopCts.Token)
        let token = linkedCts.Token

        let options = { defaultReadOptions with Subscribe = true }

        Async.Start(
            async {
                try
                    let enumerable = this.ReadAsync(filter, options, token)
                    let enumerator = enumerable.GetAsyncEnumerator(token)
                    try
                        let mutable hasMore = true
                        while hasMore do
                            let! hasValue = enumerator.MoveNextAsync().AsTask() |> Async.AwaitTask
                            if hasValue then
                                for evt in enumerator.Current.Events do
                                    onEvent evt
                            else
                                hasMore <- false
                    finally
                        (enumerator :> IAsyncDisposable).DisposeAsync().AsTask() |> Async.AwaitTask |> ignore
                finally
                    linkedCts.Dispose()
            },
            token
        )

        { new IDisposable with
            member _.Dispose() =
                stopCts.Cancel()
                stopCts.Dispose() }

    member this.AppendAsync
        (
            events: UmaEvent list,
            [<Optional>] ?failIfMatch: UmaFilter,
            [<Optional>] ?after: int64,
            [<Optional>] ?trackingInfo: UmaTrackingInfo,
            [<Optional>] ?ct: CancellationToken
        ) : Async<AppendResult> =
        async {
            let ct = defaultArg ct CancellationToken.None

            if List.isEmpty events then
                return IntegrityError "Events list cannot be empty"
            else
                try
                    let condition =
                        failIfMatch
                        |> Option.bind Filter.toProto
                        |> Option.map (fun query ->
                            { FailIfEventsMatch = query
                              After = toNullableUInt64 after })
                        |> Option.defaultValue Nullable.appendCondition

                    let trackingInfo' =
                        trackingInfo
                        |> Option.map (fun info ->
                            { TrackingInfo.Source = info.Source
                              Position = uint64 info.Position })
                        |> Option.defaultValue Nullable.trackingInfo

                    let request: AppendRequest =
                        { Events = events |> List.map Conversion.fromUmaEvent |> ResizeArray
                          Condition = condition
                          TrackingInfo = trackingInfo' }

                    let! response = service.Append(request, CallContext.op_Implicit ct).AsTask() |> Async.AwaitTask
                    return Success(int64 response.Position)
                with
                | :? RpcException as ex ->
                    let umaEx = UmaDbException.ToUmaDbException(ex)
                    match umaEx with
                    | :? IntegrityException -> return IntegrityError umaEx.Message
                    | _ -> return raise umaEx
        }

    static member Connect
        (
            host: string,
            port: int,
            [<Optional; DefaultParameterValue(null: string)>] caCert: string,
            [<Optional; DefaultParameterValue(null: string)>] apiKey: string
        ) : UmaClient =
        let opt s = if String.IsNullOrWhiteSpace s then None else Some s
        let conn = UmaConnection.create host port (opt caCert) (opt apiKey) false
        new UmaClient(conn)
