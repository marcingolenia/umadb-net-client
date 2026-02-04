using UmaDb.Csharp;
using Xunit;

namespace Tests.Csharp;

public class Connecting
{
    [Fact]
    public void can_create_uma_client()
    {
        using var umaClient = UmaClient.Connect("localhost", 50051);
    }
    
    [Fact(Skip = "Requires a key.pem file")]
    public void can_create_uma_client_with_tls()
    {
        using var umaClient = UmaClient.Connect("localhost", 50051, "~/code/key.pem");
    }

    [Fact(Skip = "Requires a key.pem file")]
    public void can_create_uma_client_with_tls_and_api_key()
    {
        using var umaClient = UmaClient.Connect("localhost", 50051, "~/code/key.pem", "my-api-key");
    }
}
