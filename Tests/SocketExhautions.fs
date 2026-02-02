module Tests.SocketExhautions

open Client
open Xunit
open UmaClient
open FsUnit.Xunit

[<Fact>]
let ``Stress Test: High concurrency should not leak sockets`` () =
    let iterations = 1000

    async {
        use client = UmaClient.Connect("localhost", 50051)

        let getHeadAsync _ = client.GetHeadAsync().AsTask() |> Async.AwaitTask

        let! results =
            List.init iterations id
            |> List.map getHeadAsync
            |> Async.Parallel

        // If we reached here without a SocketException, HTTP/2 multiplexing is working.
        results.Length |> should equal iterations
    }
    |> Async.StartAsTask