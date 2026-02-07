module Tests.SocketExhautions

open System
open Xunit
open ProtoBuf.Grpc.Client
open FsUnit.Xunit
open UmaDb.Core
open Client.UmaConnection

[<Fact>]
let ``Stress Test: High concurrency should not leak sockets`` () =
    let iterations = 1000

    async {
        AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true)
        use connection = UmaConnection.create "localhost" 50051 None None false
        let callInvoker = connection.GetCallInvoker()
        let client = callInvoker.CreateGrpcService<IDcbService>()

        let getHeadAsync _ = 
            client.Head({ _unused = Nullable() }, ProtoBuf.Grpc.CallContext.Default)
                .AsTask()
                |> Async.AwaitTask

        let! results =
            List.init iterations id
            |> List.map getHeadAsync
            |> Async.Parallel

        // If we reached here without a SocketException, HTTP/2 multiplexing is working.
        results.Length |> should equal iterations
    }
    |> Async.StartAsTask