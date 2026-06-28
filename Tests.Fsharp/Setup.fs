module Tests.Setup

open System
open System.IO
open DotNet.Testcontainers.Builders
open Xunit

type Setup() =
    do
      ContainerBuilder("ghcr.io/umadb-io/umadb:0.5.8")
        .WithName("umadb-fsharp")
        .WithPortBinding(50002, 50051)
        .WithReuse(true)
        .Build()
        .StartAsync()
        .GetAwaiter()
        .GetResult()
       
        
      ContainerBuilder("ghcr.io/umadb-io/umadb:0.5.8")
        .WithName("umadb-tls-secure-fsharp")
        .WithPortBinding(50003, 50051)
        .WithResourceMapping(new FileInfo("certs/server.pem"), "/etc/secrets/")
        .WithResourceMapping(new FileInfo("certs/server-key.pem"), "/etc/secrets/")
        .WithEnvironment("UMADB_TLS_CERT", "/etc/secrets/server.pem")
        .WithEnvironment("UMADB_TLS_KEY", "/etc/secrets/server-key.pem")
        .WithEnvironment("UMADB_API_KEY", "test-api-key")
        .WithReuse(true)
        .Build()
        .StartAsync().GetAwaiter().GetResult();

    interface IDisposable with
        member _.Dispose() = ()

[<assembly: CaptureConsole>]
do ()

[<assembly: AssemblyFixture(typeof<Setup>)>]
do ()
