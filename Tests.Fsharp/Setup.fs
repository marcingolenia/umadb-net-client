module Tests.Setup

open System
open System.IO
open DotNet.Testcontainers.Builders
open Xunit

/// Writes to the raw stderr stream: [<CaptureConsole>] redirects Console.Error while tests run, which could swallow the output.
let private logToStderr (message: string) =
    use stderr = Console.OpenStandardError()
    let bytes = Text.Encoding.UTF8.GetBytes(message + Environment.NewLine)
    stderr.Write(bytes, 0, bytes.Length)
    stderr.Flush()

type Setup() =
    do
      // xunit only reports "[FATAL ERROR] <ExceptionType>" for a crash on a background thread; log the full exception.
      AppDomain.CurrentDomain.UnhandledException.Add(fun args ->
          logToStderr $"[UNHANDLED EXCEPTION] terminating={args.IsTerminating}{Environment.NewLine}{args.ExceptionObject}")
      System.Threading.Tasks.TaskScheduler.UnobservedTaskException.Add(fun args ->
          logToStderr $"[UNOBSERVED TASK EXCEPTION]{Environment.NewLine}{args.Exception}")
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
