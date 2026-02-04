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
    public async Task cannot_create_uma_client_without_tls_if_server_requires_tls()
    {
        using var umaClient = UmaClient.Connect("localhost", 50001);
        var exception = await Assert.ThrowsAsync<UmaDbException>(() => umaClient.GetHeadAsync().AsTask());
        Assert.IsType<UmaDbException>(exception);
    }
    
    [Fact]
    public async Task cannot_create_uma_client_without_apikey_if_server_requires_tls_with_apikey()
    {
        using var umaClient = UmaClient.Connect("localhost", 50001, "certs/ca.pem");
        var exception = await Assert.ThrowsAsync<UmaDbException.AuthenticationException>(() => umaClient.GetHeadAsync().AsTask());
        Assert.IsType<UmaDbException.AuthenticationException>(exception);
        Assert.Equal("Authentication error: missing or invalid API key", exception.Message);
    }

    [Fact]
    public async Task can_create_uma_client_with_tls_and_api_key()
    {
        using var umaClient = UmaClient.Connect("localhost", 50001, "certs/ca.pem", "test-api-key");
        await umaClient.GetHeadAsync();
    }
}
