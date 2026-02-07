module Tests.Setup

open System
open DotNet.Testcontainers.Builders
open Xunit

type Setup() =
    // do
        // let umaDb =
        //       ContainerBuilder("ghcr.io/umadb-io/umadb:latest")
        //         .WithName("umadb")
        //         .WithPortBinding(50051, 50051)
        //         .WithReuse(true)
        //         .Build()

        // umaDb.StartAsync() |> Async.AwaitTask |> Async.RunSynchronously

    interface IDisposable with
        member _.Dispose() = ()

[<assembly: CaptureConsole>]
do ()

[<assembly: AssemblyFixture(typeof<Setup>)>]
do ()
