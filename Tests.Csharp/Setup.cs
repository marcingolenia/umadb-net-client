using DotNet.Testcontainers.Builders;
using Xunit;

[assembly: CaptureConsole]
[assembly: AssemblyFixture(typeof(Tests.Csharp.Setup))]

namespace Tests.Csharp
{
    public class Setup : IDisposable
    {
        public Setup()
        {
            new ContainerBuilder("ghcr.io/umadb-io/umadb:latest")
                .WithName("umadb")
                .WithPortBinding(50051, 50051)
                .WithReuse(true)
                .Build()
                .StartAsync().GetAwaiter().GetResult();
        }

        public void Dispose()
        {
        }
    }
}