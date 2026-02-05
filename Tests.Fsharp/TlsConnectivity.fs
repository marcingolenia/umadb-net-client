module TlsConnectivity

open System
open FsUnit.Xunit
open UmaDb.Core
open Xunit
open Client.UmaConnection
open Client.UmaClient

let private appendRequest  =
    let event =
        { EventType = "TestEvent"
          Tags = ResizeArray([ "test"; "tls" ])
          Data = System.Text.Encoding.UTF8.GetBytes("Hello UmaDB")
          Uuid = Guid.NewGuid().ToString() }
    { Events = ResizeArray([ event ])
      Condition = Nullable.appendCondition
      TrackingInfo = Nullable.trackingInfo }

[<Fact>]
let ``API key without CA cert is rejected`` () =
    (fun () -> UmaConnection.create "localhost" 50051 None (Some "secret-key") |> ignore)
    |> should throw (typeof<ArgumentException>)

[<Fact>]
let ``Can connect and append using UmaClient over plain HTTP`` () =
    task {
        use client = UmaClient.Connect("localhost", 50051)
        let! response = client.AppendAsync(appendRequest)
        response.Position |> should be (greaterThan 0UL)
    }
