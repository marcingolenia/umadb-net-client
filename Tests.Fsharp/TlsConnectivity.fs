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
      Condition = Nullable.appendCondition }

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

[<Fact(Skip = "Set UMADB_TLS_CA_CERT and optionally UMADB_TLS_PORT to run against a TLS server")>]
let ``Can connect and append using UmaClient over TLS with CA cert`` () =
    task {
        let caCert = Environment.GetEnvironmentVariable("UMADB_TLS_CA_CERT")
        if String.IsNullOrWhiteSpace caCert then
            failwith "UMADB_TLS_CA_CERT must be set to path to CA cert file"
        let port =
            match Environment.GetEnvironmentVariable("UMADB_TLS_PORT") with
            | null
            | "" -> 50451
            | s -> int s
        let host = Environment.GetEnvironmentVariable("UMADB_TLS_HOST") |> Option.ofObj |> Option.defaultValue "localhost"
        let apiKey = Environment.GetEnvironmentVariable("UMADB_TLS_API_KEY") |> Option.ofObj
        use client = UmaClient.Connect(host, port, caCert, Option.defaultValue null apiKey)
        let! _ = client.GetHeadAsync()
        let! response = client.AppendAsync(appendRequest)
        response.Position |> should be (greaterThan 0UL)
    }
