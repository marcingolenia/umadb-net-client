module Tests.Client.Connecting

open System.Threading
open Xunit
open UmaDb.Fsharp.ConnectionBuilder

[<Fact>]
let ``Can connect`` () =
    task {
        use umaClient = connect "localhost" 50002 |> build
        let! head = umaClient |> UmaDb.Fsharp.Client.head CancellationToken.None
        printf $"%A{head}"
    }

    