using DotNet.Testcontainers.Builders;
using Microsoft.Extensions.Logging;
using Xunit;

[assembly: CaptureConsole]
[assembly: AssemblyFixture(typeof(Tests.Csharp.Setup))]


namespace Tests.Csharp
{
    public class Setup : IDisposable
    {
        const string ENV_UMADB_API_KEY = "UMADB_API_KEY";
        const string ENV_UMADB_TLS_CERT = "UMADB_TLS_CERT";
        const string ENV_UMADB_TLS_KEY = "UMADB_TLS_KEY";
        const string TLS_SECRETS_DIR = "/etc/secrets";
        const string TLS_CERT_CONTAINER_PATH = "/etc/secrets/server.pem";
        const string TLS_KEY_CONTAINER_PATH = "/etc/secrets/server-key.pem";

        static readonly object Gate = new();
        static bool ContainersStarted;

        static ILogger CreateLogger()
        {
            var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder
                    .SetMinimumLevel(LogLevel.Trace)
                    .AddSimpleConsole(options =>
                    {
                        options.SingleLine = true;
                        options.TimestampFormat = "[HH:mm:ss] ";
                    });
            });
            return loggerFactory.CreateLogger("Testcontainers");
        }

        public Setup()
        {
            lock (Gate)
            {
                if (ContainersStarted) return;
                ContainersStarted = true;
            }

            var logger = CreateLogger();

            new ContainerBuilder("ghcr.io/umadb-io/umadb:latest")
                .WithName("umadb")
                .WithPortBinding(50051, 50051)
                .WithLogger(logger)
                .WithReuse(true)
                .Build()
                .StartAsync().GetAwaiter().GetResult();

            new ContainerBuilder("ghcr.io/umadb-io/umadb:latest")
                .WithName("umadb-tls-secure")
                .WithLogger(logger)
                .WithPortBinding(50001, 50051)
                .WithResourceMapping(new FileInfo("certs/server.pem"), TLS_SECRETS_DIR + "/")
                .WithResourceMapping(new FileInfo("certs/server-key.pem"), TLS_SECRETS_DIR + "/")
                .WithEnvironment(ENV_UMADB_TLS_CERT, TLS_CERT_CONTAINER_PATH)
                .WithEnvironment(ENV_UMADB_TLS_KEY, TLS_KEY_CONTAINER_PATH)
                .WithEnvironment(ENV_UMADB_API_KEY, "test-api-key")
                .WithReuse(true)
                .Build()
                .StartAsync().GetAwaiter().GetResult();
        }

        public void Dispose()
        {
        }
    }
}