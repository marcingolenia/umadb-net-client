module Operations

open System
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
      Condition = None }

[<Fact>]
let ``Can write and read`` () =
    task {
        use client = UmaClient.Connect("localhost", 50051)
        let! writeReponse = client.AppendAsync(appendRequest)

        let readRequest: ReadRequest =
            { Query =
                Some
                    { Items =
                        [ { Tags = [ "operations" ] |> ResizeArray
                            Types = [ "TestEvent" ] |> ResizeArray } ]
                        |> ResizeArray }
              Start = None
              Backwards = false
              Limit = None
              Subscribe = false
              BatchSize = None }

        let! events = client.ReadListAsync(readRequest)


        writeReponse.Position |> should be (greaterThan 0UL)
    }
