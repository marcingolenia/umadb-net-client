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
        let tags = ["tag1"; "tag2"]
        let expectedEvent = { EventType = Guid.NewGuid().ToString()
                              Data = ReadOnlyMemory(Encoding.UTF8.GetBytes "test")
                              Tags = Some tags
                              Id = None }
        let uma = connect "localhost" 50002 |> build
        let events: UmaEvent list = [ expectedEvent ]
        // Act
        let! appendResponse =
            appendOperation events
            |> append uma CancellationToken.None
        let! events, position = readList uma [{Tags = tags; Types = [expectedEvent.EventType]}]
        // Assert
        match appendResponse with
        | Ok head -> head |> should be (greaterThan 0L)
        | Error err -> failwith (err.ToString())
        events.Length |> should equal 1
        let actualEvent = events[0].Event
        actualEvent.EventType |> should equal expectedEvent.EventType
        actualEvent.Data.ToArray() |> should equal (expectedEvent.Data.ToArray())
        actualEvent.Tags |> should equal expectedEvent.Tags
        actualEvent.Id |> Option.isSome |> should be True
        position |> Option.isSome |> should be True
        position |> Option.iter (fun p -> p |> should be (greaterThanOrEqualTo 0L))
    }

[<Fact>]
let ``When events are appended and failCondition fails then IntegrationError is returned`` () =
    task {
        // Arrange
        let tags = ["tag1"; "tag2"]
        let expectedEvent = { EventType = Guid.NewGuid().ToString()
                              Data = ReadOnlyMemory(Encoding.UTF8.GetBytes "test")
                              Tags = Some tags
                              Id = None }
        let query = [{Tags = tags; Types = [expectedEvent.EventType]}]
        let uma = connect "localhost" 50002 |> build
        let events: UmaEvent list = [ expectedEvent ]
        // Act
        let! firstAppendResponse =
            appendOperation events
            |> failIfMatch query
            |> append uma CancellationToken.None
        let! secondAppendResponse =
            appendOperation events
            |> failIfMatch query
            |> append uma CancellationToken.None
        // Assert
        firstAppendResponse.IsOk |> should be True
        secondAppendResponse.IsError |> should be True
        match secondAppendResponse with
        | Ok _ -> failwith "Expected Error (IntegrityError.ErrorMessage)"
        | Error (Errors.IntegrityError.ErrorMessage msg) ->  msg |> should haveSubstring "condition failed: condition: "
    }
    
[<Fact>]
let ``idempotent append with same id returns same commit position`` () =
    task {
        // Arrange
        let uma = connect "localhost" 50002 |> build
        let tags = ["tag1"; "tag2"]
        let evtType = "WhateverType"
        let query: Query = [{Tags=tags; Types = [evtType]}]
        let id = Guid.NewGuid()
        let! _, after = readList uma query
        let evt1 = { EventType = evtType
                     Data = ReadOnlyMemory(Encoding.UTF8.GetBytes "test")
                     Tags = Some tags
                     Id = Some id }
        let evt2 = { EventType = evtType
                     Data = ReadOnlyMemory(Encoding.UTF8.GetBytes "test")
                     Tags = Some tags
                     Id = Some id }
        // Act
        let! appendResponse1 = append uma CancellationToken.None (appendOperation [evt1] |> failIfMatch query |> withAfter after)
        let! appendResponse2 = append uma CancellationToken.None (appendOperation [evt2] |> failIfMatch query |> withAfter after)
        // Assert
        appendResponse1.IsOk |> should be True
        appendResponse2.IsOk |> should be True
        appendResponse1 |> should equal appendResponse2
    }

