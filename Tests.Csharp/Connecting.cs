using UmaDb.Csharp;
using Xunit;

namespace Tests.Csharp;

public class Connecting
{
    [Fact]
    public async Task can_create_uma_client()
    {
        using var umaClient = UmaClient.Connect("localhost", 50051);
        await umaClient.GetHeadAsync();
    }
    
    [Fact]
    public async Task can_create_uma_client_with_tls()
    {
        using var umaClient = UmaClient.Connect("localhost", 50001, "certs/ca.pem");
        await umaClient.GetHeadAsync();
    }
    
    [Fact]
    public void cannot_create_uma_client_without_tls_if_server_requires_tls()
    {
        using var umaClient = UmaClient.Connect("localhost", 50001);
    }

    [Fact]
    public async Task can_create_uma_client_with_tls_and_api_key()
    {
        using var umaClient = UmaClient.Connect("localhost", 50001, "certs/ca.pem", "test-api-key");
        await umaClient.GetHeadAsync();
    }
}
