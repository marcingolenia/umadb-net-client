module Operations

open System
open System.Threading.Tasks
open FsUnit.Xunit
open UmaDb.Core
open Xunit
open Client.UmaClient

let event =
    { EventType = "TestEvent"
      Tags = ResizeArray([ "operations"; "operations-2" ])
      Data = System.Text.Encoding.UTF8.GetBytes("Hello UmaDB")
      Uuid = Guid.NewGuid().ToString() }

let appendRequest =
    { Events = ResizeArray([ event ])
      Condition = Nullable.appendCondition }

[<Fact>]
let ``Can write and read`` () =
    task {
        use client = UmaClient.Connect("localhost", 50051)
        let! writeReponse = client.AppendAsync(appendRequest)

        let query =
            { Items =
                [ { Types = [ "TestEvent" ] |> ResizeArray
                    Tags = [ "operations" ] |> ResizeArray } ]
                |> ResizeArray }
        let readRequest: ReadRequest =
            { Query = query
              Start = Nullable 1UL
              Backwards = false
              Limit = Nullable()
              Subscribe = false
              BatchSize = Nullable() }

        let! events = client.ReadListAsync(readRequest)

        events.Count |> should be (greaterThan 0)
        writeReponse.Position |> should be (greaterThan 0UL)
    }
