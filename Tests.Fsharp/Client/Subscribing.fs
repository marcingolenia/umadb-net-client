module Tests.Client.Subscribing

open System
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open FSharp.Control
open FsUnit.Xunit
open Xunit
open UmaDb.Client.ClientBuilder
open UmaDb.Client.Event
open UmaDb.Client.Query
open UmaDb.Client.Operations

type OrderCreated = { OrderId: Guid; Amount: decimal }

let private assertReceivedOrderCreated (received: SequencedUmaEvent) (tag: string) (expected: OrderCreated) =
    received.Event.EventType |> should equal "OrderCreated"
    received.Event.Tags |> Option.iter (fun tags -> tags |> should contain tag)
    let payload = JsonSerializer.Deserialize<OrderCreated>(received.Event.Data.ToArray())
    payload.OrderId |> should equal expected.OrderId
    payload.Amount |> should equal expected.Amount

[<Fact>]
let ``can subscribe to events using subscribeWithCallback`` () =
    task {
        use umaClient = connect "localhost" 50002 |> build
        let tag = $"subscribe-cb-{Guid.NewGuid()}"
        let orderCreated = { OrderId = Guid.NewGuid(); Amount = 42m }
        let eventToAppend =
            { EventType = "OrderCreated"
              Data = ReadOnlyMemory(JsonSerializer.SerializeToUtf8Bytes orderCreated )
              Tags = Some [ tag ]
              Id = None }

        let received = TaskCompletionSource<SequencedUmaEvent>()
        let ct = CancellationToken.None
        let query = [ { Types = [ "OrderCreated" ]; Tags = [ tag ] } ]

        use _subscription =
            subscribeWithCallback 
                umaClient 
                ct 
                query 
                (fun (evt, _) ->
                    task {
                        received.TrySetResult(evt) |> ignore
                    })

        let! _ = appendOperation [ eventToAppend ] |> append umaClient ct
        let! evt = received.Task
        assertReceivedOrderCreated evt tag orderCreated
    }

[<Fact>]
let ``can subscribe using read with subscribe option`` () =
    task {
        use umaClient = connect "localhost" 50002 |> build
        let tag = $"subscribe-async-{Guid.NewGuid()}"
        let orderCreated = { OrderId = Guid.NewGuid(); Amount = 99m }
        let eventToAppend =
            { EventType = "OrderCreated"
              Data = ReadOnlyMemory(JsonSerializer.SerializeToUtf8Bytes(orderCreated))
              Tags = Some [ tag ]
              Id = None }

        let received = TaskCompletionSource<SequencedUmaEvent>()
        let ct = CancellationToken.None
        let query = [ { Types = [ "OrderCreated" ]; Tags = [ tag ] } ]
        let options = QueryOptions.defaults |> QueryOptions.subscribe

        let _ =
            Task.Run(fun () ->
                task {
                    for evt in readWithOptions umaClient ct query options do
                        received.TrySetResult(evt) |> ignore
                } :> Task)

        let! _ = appendOperation [ eventToAppend ] |> append umaClient ct
        let! evt = received.Task
        assertReceivedOrderCreated evt tag orderCreated
    }
