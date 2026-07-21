using DotNet.Testcontainers.Builders;
using Microsoft.Extensions.Logging;
using Xunit;

[assembly: CaptureConsole]
[assembly: AssemblyFixture(typeof(Tests.Csharp.Setup))]


namespace Tests.Csharp
{
    public class Setup : IDisposable
    {
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
            var logger = CreateLogger();

            new ContainerBuilder("ghcr.io/umadb-io/umadb:0.6.6")
                .WithName("umadb")
                .WithPortBinding(50051, 50051)
                .WithLogger(logger)
                .WithReuse(true)
                .Build()
                .StartAsync().GetAwaiter().GetResult();

            new ContainerBuilder("ghcr.io/umadb-io/umadb:0.6.6")
                .WithName("umadb-tls-secure")
                .WithLogger(logger)
                .WithPortBinding(50001, 50051)
                .WithResourceMapping(new FileInfo("certs/server.pem"), "/etc/secrets/")
                .WithResourceMapping(new FileInfo("certs/server-key.pem"), "/etc/secrets/")
                .WithEnvironment("UMADB_TLS_CERT", "/etc/secrets/server.pem")
                .WithEnvironment("UMADB_TLS_KEY", "/etc/secrets/server-key.pem")
                .WithEnvironment("UMADB_API_KEY", "test-api-key")
                .WithReuse(true)
                .Build()
                .StartAsync().GetAwaiter().GetResult();
        }

        public void Dispose()
        {
        }
    }
}