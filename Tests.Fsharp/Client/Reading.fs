module Tests.Client.Reading

open System
open System.Text
open System.Threading
open System.Threading.Tasks
open FSharp.Control
open FsUnit.Xunit
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

[<Fact>]
let ``When events were appended then reading with correct types and tags retrieve them`` () =
    task {
        // Arrange
        let expectedEvent = { EventType = "Type1"
                              Data = ReadOnlyMemory(Encoding.UTF8.GetBytes "test")
                              Tags = Some ["tag1"; "tag2"]
                              Id = None }
        let uma = connect "localhost" 50002 |> build
        let events: UmaEvent list = [ expectedEvent ]
        // Act
        let! appendResponse =
            appendOperation events
            |> track "test" 1L
            |> failIfMatch []
            |> after 4L
            |> append uma CancellationToken.None
        let! (_events, _position) = readList uma []
        // Assert
        match appendResponse with
        | Ok head -> head |> should be (greaterThan 0L)
        | Error err -> failwith (err.ToString())
    }