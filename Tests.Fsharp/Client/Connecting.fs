module Tests.Client.Connecting

open System
open System.IO
open System.Security.Cryptography.X509Certificates
open System.Threading
open System.Threading.Tasks
open FSharp.Control
open Xunit
open UmaDb.Fsharp.ConnectionBuilder
open UmaDb.Fsharp.Client
open Errors

[<Fact>]
let ``Connect throws on empty host`` () =
    let ex = Assert.Throws<ArgumentException>(fun () -> connect "" 50051 |> build |> ignore)
    Assert.Equal("host", ex.ParamName)

[<Fact>]
let ``Connect throws on invalid port`` () =
    let ex = Assert.Throws<ArgumentException>(fun () -> connect "localhost" 0 |> build |> ignore)
    Assert.Equal("port", ex.ParamName)


[<Fact>]
let ``can create uma client to http server`` () =
    task {
        use umaClient = connect "localhost" 50002 |> build
        let! _ = head TestContext.Current.CancellationToken umaClient
        ()
    }

[<Fact>]
let ``cannot_connect_with_tls_to_http_only_server`` () =
    (fun () ->
        task {
            use client = connect "localhost" 50002 |> withTls |> build
            let! _ = head TestContext.Current.CancellationToken client
            ()
        }
        :> Task)
    |> Assert.ThrowsAnyAsync<UmaDbException>

[<Fact>]
let ``cannot connect with api key to http only server`` () =
    (fun () ->
        task {
            use umaClient = connect "localhost" 50002 |> withApiKey "key" |> build
            let! _ = head TestContext.Current.CancellationToken umaClient
            ()
        }
        :> Task)
    |> Assert.ThrowsAnyAsync<UmaDbException>

[<Fact>]
let ``cannot create uma client without tls if server requires tls`` () =
    task {
        let work () =
            task {
                use umaClient = connect "localhost" 50003 |> build
                let! _ = head TestContext.Current.CancellationToken umaClient
                ()
            }
            :> Task
        let! _ex = Assert.ThrowsAsync<UmaDbException>(work)
        ()
    }

[<Fact>]
let ``cannot create uma client without apikey if server requires tls with apikey`` () =
    task {
        let work () =
            task {
                use umaClient = connect "localhost" 50003 |> withCaCert "certs/ca.pem" |> build
                let! _ = head TestContext.Current.CancellationToken umaClient
                ()
            }
            :> Task
        let! ex = Assert.ThrowsAsync<AuthenticationException>(work)
        Assert.Equal("Authentication error: missing or invalid API key", ex.Message)
    }

[<Fact>]
let ``cannot create uma client with wrong apikey`` () =
    task {
        let work () =
            task {
                use umaClient = connect "localhost" 50003 |> withCaCert "certs/ca.pem" |> withApiKey "wrong-api-key" |> build
                let! _ = head TestContext.Current.CancellationToken umaClient
                ()
            }
            :> Task
        let! ex = Assert.ThrowsAsync<AuthenticationException>(work)
        Assert.Equal("Authentication error: missing or invalid API key", ex.Message)
    }

[<Fact>]
let ``can create uma client to tls server with api key`` () =
    task {
        use umaClient = connect "localhost" 50003 |> withCaCert "certs/ca.pem" |> withApiKey "test-api-key" |> build
        let! _ = head CancellationToken.None umaClient
        ()
    }

[<Fact>]
let ``can connect with well known ca and api key`` () =
    task {
        let caPath = Path.Combine(AppContext.BaseDirectory, "certs", "ca.pem")
        if not (File.Exists(caPath)) then
            raise (InvalidOperationException($"Test CA not found at {caPath}. Ensure certs are copied to output."))
        let certPem = File.ReadAllText(caPath)
        use caCert = X509Certificate2.CreateFromPem(certPem)
        use store = new X509Store(StoreName.Root, StoreLocation.CurrentUser)
        store.Open OpenFlags.ReadWrite
        store.Add caCert
        try
            use umaClient = connect "localhost" 50003 |> withTls |> withApiKey "test-api-key" |> build
            let! _ = head CancellationToken.None umaClient
            ()
        finally
            store.Remove(caCert)
    }
