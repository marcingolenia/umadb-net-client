module Tests.Client.Reading

open System
open System.Collections.Generic
open System.Threading
open Xunit
open Grpc.Core
open UmaDb.Fsharp.ConnectionBuilder
open UmaDb.Fsharp.Client
open UmaDb.Client.Types

let rec private isCancellationException (ex: exn) =
    match ex with
    | :? OperationCanceledException -> true
    | :? AggregateException as ae ->
        ae.Flatten().InnerExceptions |> Seq.exists isCancellationException
    | :? RpcException as rpc when rpc.StatusCode = StatusCode.Cancelled -> true
    | _ -> false

/// Drains the read stream with the given token so cancellation is observed during iteration.
let private drainWithToken (stream: IAsyncEnumerable<SequencedUmaEvent>) (ct: CancellationToken) =
    let e = stream.GetAsyncEnumerator(ct)
    let rec loop () =
        async {
            let! more = e.MoveNextAsync().AsTask() |> Async.AwaitTask
            if more then
                return! loop ()
        }
    async {
        try
            do! loop ()
        finally
            e.DisposeAsync().AsTask().GetAwaiter().GetResult()
    }

[<Fact>]
let ``Read throws when cancellation is requested before starting`` () =
    use umaClient = connect "localhost" 50002 |> build
    use cts = new CancellationTokenSource()
    cts.Cancel()

    let query: Query = []
    let options = QueryOptions.defaults

    let work =
        async {
            let! stream = readWithOptions query options umaClient
            do! drainWithToken stream cts.Token
        }

    Assert.Throws<OperationCanceledException>(fun () ->
        Async.RunSynchronously(work, cancellationToken = cts.Token)
        |> ignore)

[<Fact>]
let ``Read throws when cancelled during stream`` () =
    use umaClient = connect "localhost" 50002 |> build
    use cts = new CancellationTokenSource()
    cts.CancelAfter(150)

    let query: Query = []
    let options = QueryOptions.defaults |> QueryOptions.subscribe

    let work =
        async {
            let! stream = readWithOptions query options umaClient
            do! drainWithToken stream cts.Token
        }

    let mutable thrown = None
    try
        Async.RunSynchronously(work, cancellationToken = cts.Token) |> ignore
    with ex ->
        thrown <- Some ex

    match thrown with
    | Some ex ->
        Assert.True(isCancellationException ex, $"Expected cancellation, got: {ex}")
    | None ->
        Assert.Fail("Expected an exception to be thrown")
