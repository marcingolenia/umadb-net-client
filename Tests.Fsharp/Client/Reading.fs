module Tests.Client.Reading

open System
open System.Threading
open System.Threading.Tasks
open FSharp.Control
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

    let work () =
        readWithOptions query options umaClient cts.Token
        |> TaskSeq.iter (fun _ -> ())
        :> Task

    Assert.ThrowsAsync<OperationCanceledException>(work)

[<Fact>]
let ``Read throws when cancelled during stream`` () =
    use umaClient = connect "localhost" 50002 |> build
    let query: Query = []
    let options = QueryOptions.defaults |> QueryOptions.subscribe

    let work () =
        task {
            use ctsInner = new CancellationTokenSource()
            let seq = readWithOptions query options umaClient ctsInner.Token
            do! (TaskSeq.iter (fun _ -> ctsInner.Cancel()) seq |> Async.AwaitTask)
        }
        :> Task

    Assert.ThrowsAsync<Errors.CancelledException>(work) |> ignore

