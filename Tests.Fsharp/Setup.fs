module Tests.Setup

open System
open System.IO
open DotNet.Testcontainers.Builders
open Xunit

/// vstest does not surface the test host's stderr and [<CaptureConsole>] redirects Console, so crash details go to a file
/// next to the test assembly. The CI workflow prints it.
let private crashLogPath = IO.Path.Combine(AppContext.BaseDirectory, "unhandled-exceptions.log")

let private logCrash (message: string) =
    IO.File.AppendAllText(crashLogPath, $"{DateTime.UtcNow:O} {message}{Environment.NewLine}{Environment.NewLine}")

type Setup() =
    do
      // xunit only reports "[FATAL ERROR] NullReferenceException" (no stack) and its own UnhandledException handler runs
      // before any we could register, so log every NullReferenceException at throw time, with the full stack of the throwing thread.
      AppDomain.CurrentDomain.FirstChanceException.Add(fun args ->
          match args.Exception with
          | :? NullReferenceException as ex ->
              try logCrash $"[NullReferenceException thrown]{Environment.NewLine}{ex}{Environment.NewLine}--- stack of throwing thread ---{Environment.NewLine}{Environment.StackTrace}"
              with _ -> ()
          | _ -> ())
      // Exceptions from fire-and-forget tasks are otherwise swallowed silently.
      System.Threading.Tasks.TaskScheduler.UnobservedTaskException.Add(fun args ->
          logCrash $"[UNOBSERVED TASK EXCEPTION]{Environment.NewLine}{args.Exception}")
      ContainerBuilder("ghcr.io/umadb-io/umadb:0.7.8")
        .WithName("umadb-fsharp")
        .WithPortBinding(50002, 50051)
        .WithReuse(true)
        .Build()
        .StartAsync()
        .GetAwaiter()
        .GetResult()
       
        
      ContainerBuilder("ghcr.io/umadb-io/umadb:0.7.8")
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
