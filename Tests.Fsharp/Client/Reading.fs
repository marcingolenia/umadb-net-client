module Tests.Client.Reading

open System
open System.Threading
open Xunit
open UmaDb.Fsharp.ConnectionBuilder
open UmaDb.Fsharp.Client
open UmaDb.Client.Types

[<Fact>]
let ``Read throws when cancellation is requested before starting`` () =
    use umaClient = connect "localhost" 50002 |> build
    use cts = new CancellationTokenSource()
    cts.Cancel()

    let query: Query = []
    let options = QueryOptions.defaults

    let work =
        async {
            let! _ = readWithOptions query options cts.Token umaClient
            ()
        }

    Assert.Throws<OperationCanceledException>(fun () ->
        Async.RunSynchronously(work, cancellationToken = cts.Token))

[<Fact>]
let ``Read throws when cancelled during stream`` () =
    use umaClient = connect "localhost" 50002 |> build
    use cts = new CancellationTokenSource()
    let query: Query = []
    let options = QueryOptions.defaults |> QueryOptions.subscribe

    let work =
        async {
            let! _ = readWithOptions query options cts.Token umaClient
            cts.Cancel()
            ()
        }

    Assert.Throws<OperationCanceledException>(fun () ->
        Async.RunSynchronously(work, cancellationToken = cts.Token))
