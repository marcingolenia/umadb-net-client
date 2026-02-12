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
open UmaDb.Fsharp.Types
open UmaDb.Fsharp

[<Fact>]
let ``Read throws when cancellation is requested before starting`` () =
    use umaClient = connect "localhost" 50002 |> build
    use cts = new CancellationTokenSource()
    cts.Cancel()

    let query: Query = []
    let options = QueryOptions.defaults

    let work () =
        readWithOptions umaClient cts.Token query options
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
            let seq = readWithOptions umaClient ctsInner.Token query options
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

[<Fact>]
let ``can get head position`` () =
    task {
        use uma = connect "localhost" 50002 |> build
        let! head = readHead uma CancellationToken.None
        head |> Option.isSome |> should be True
    }

[<Fact>]
let ``append returns commit position`` () =
    task {
        use uma = connect "localhost" 50002 |> build
        let tag = $"pos-{Guid.NewGuid()}"
        let evt = { EventType = "OrderCreated"
                    Data = ReadOnlyMemory Array.empty
                    Tags = Some [tag]
                    Id = None }
        let! response = appendOperation [evt] |> append uma CancellationToken.None
        match response with
        | Ok position -> position |> should be (greaterThan 0L)
        | Error err -> failwith (err.ToString())
    }

[<Fact>]
let ``can read backwards`` () =
    task {
        use uma = connect "localhost" 50002 |> build
        let tag = $"back-{Guid.NewGuid()}"
        let events =
            [ { EventType = "A"; Data = ReadOnlyMemory [|1uy|]; Tags = Some [tag]; Id = None }
              { EventType = "B"; Data = ReadOnlyMemory [|2uy|]; Tags = Some [tag]; Id = None }
              { EventType = "C"; Data = ReadOnlyMemory [|3uy|]; Tags = Some [tag]; Id = None } ]
        let! _ = appendOperation events |> append uma CancellationToken.None
        let query = [{ Tags = [tag]; Types = ["A"; "B"; "C"] }]
        let options = QueryOptions.defaults |> QueryOptions.backwards |> QueryOptions.limit 2
        let! eventsRead =
            readWithOptions uma CancellationToken.None query options
            |> TaskSeq.toListAsync
        eventsRead.Length |> should equal 2
        eventsRead[0].Event.EventType |> should equal "C"
        eventsRead[1].Event.EventType |> should equal "B"
    }

[<Fact>]
let ``consistency boundary read then append with condition after head`` () =
    task {
        use uma = connect "localhost" 50002 |> build
        let tag = $"cb-{Guid.NewGuid()}"
        let evtType = "OrderCreated"
        let query: Query = [{ Tags = [tag]; Types = [evtType] }]
        let evt1 = { EventType = evtType
                     Data = ReadOnlyMemory(Encoding.UTF8.GetBytes """{"OrderId":"00000000-0000-0000-0000-000000000001","Amount":1}""")
                     Tags = Some [tag]
                     Id = None }
        let! _ = appendOperation [evt1] |> failIfMatch query |> append uma CancellationToken.None
        do! readWithOptions uma CancellationToken.None query QueryOptions.defaults
            |> TaskSeq.iter (fun _ -> ())
        let! after = readHead uma CancellationToken.None
        let evt2 = { EventType = evtType
                     Data = ReadOnlyMemory(Encoding.UTF8.GetBytes """{"OrderId":"00000000-0000-0000-0000-000000000002","Amount":2}""")
                     Tags = Some [tag]
                     Id = None }
        let! _ = appendOperation [evt2] |> failIfMatch query |> withAfter after |> append uma CancellationToken.None
        let! events, _ = readList uma query
        events.Length |> should equal 2
    }

