module Tests.Client.Connecting

open Xunit
open UmaDb.Fsharp.ConnectionBuilder

[<Fact>]
let ``Can connect`` () =
    async {
        use umaClient = connect "localhost" 50002 |> build
        let! head = umaClient |> UmaDb.Fsharp.Client.head
        printf $"%A{head}"
    }

    