using System.Security.Cryptography.X509Certificates;
using UmaDb.Client;
using Xunit;

namespace Tests.Csharp;

public class Connecting
{
    [Fact]
    public void Connect_throws_on_null_options()
    {
        var ex = Assert.Throws<ArgumentNullException>(() => UmaClient.Connect(null!));
        Assert.Equal("options", ex.ParamName);
    }

    [Fact]
    public void Connect_throws_on_empty_host()
    {
        var ex = Assert.Throws<ArgumentException>(() =>
            UmaClient.Connect(new UmaClientOptions().WithHost("").WithPort(50051)));
        Assert.Equal("options", ex.ParamName);
    }

    [Fact]
    public void Connect_throws_on_invalid_port()
    {
        var ex = Assert.Throws<ArgumentOutOfRangeException>(() =>
            UmaClient.Connect(new UmaClientOptions().WithHost("localhost").WithPort(0)));
        Assert.Equal("options", ex.ParamName);
    }

    [Fact]
    public async Task can_create_uma_client_to_http_server()
    {
        using var umaClient = UmaClient.Connect(new UmaClientOptions().WithHost("localhost").WithPort(50051));
        await umaClient.GetHeadAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task cannot_connect_with_tls_to_http_only_server()
    {
        using var umaClient =
            UmaClient.Connect(new UmaClientOptions().WithHost("localhost").WithPort(50051).EnableTls());
        await Assert.ThrowsAnyAsync<UmaDbException>(() =>
            umaClient.GetHeadAsync(TestContext.Current.CancellationToken).AsTask());
    }

    [Fact]
    public async Task cannot_connect_with_api_key_to_http_only_server()
    {
        using var umaClient =
            UmaClient.Connect(new UmaClientOptions().WithHost("localhost").WithPort(50051).WithApiKey("key"));
        await Assert.ThrowsAnyAsync<UmaDbException>(() =>
            umaClient.GetHeadAsync(TestContext.Current.CancellationToken).AsTask());
    }

    [Fact]
    public async Task cannot_create_uma_client_without_tls_if_server_requires_tls()
    {
        using var umaClient = UmaClient.Connect(new UmaClientOptions().WithHost("localhost").WithPort(50001));

        await Assert.ThrowsAnyAsync<UmaDbException>(() =>
            umaClient.GetHeadAsync(TestContext.Current.CancellationToken).AsTask());
    }

    [Fact]
    public async Task cannot_create_uma_client_without_apikey_if_server_requires_tls_with_apikey()
    {
        using var umaClient =
            UmaClient.Connect(new UmaClientOptions().WithHost("localhost").WithPort(50001).WithCaCert("certs/ca.pem"));

        var exception = await Assert.ThrowsAsync<UmaDbException.AuthenticationException>(() =>
            umaClient.GetHeadAsync(TestContext.Current.CancellationToken).AsTask());
        Assert.IsType<UmaDbException.AuthenticationException>(exception);
        Assert.Equal("authentication error: missing or invalid API key", exception.Message);
    }

    [Fact]
    public async Task cannot_create_uma_client_with_wrong_apikey()
    {
        using var umaClient = UmaClient.Connect(new UmaClientOptions().WithHost("localhost").WithPort(50001)
            .WithCaCert("certs/ca.pem").WithApiKey("wrong-api-key"));

        var exception = await Assert.ThrowsAsync<UmaDbException.AuthenticationException>(() =>
            umaClient.GetHeadAsync(TestContext.Current.CancellationToken).AsTask());
        Assert.IsType<UmaDbException.AuthenticationException>(exception);
        Assert.Equal("authentication error: missing or invalid API key", exception.Message);
    }

    [Fact]
    public async Task can_create_uma_client_to_tls_server_with_api_key()
    {
        using var umaClient = UmaClient.Connect(new UmaClientOptions().WithHost("localhost").WithPort(50001)
            .WithCaCert("certs/ca.pem").WithApiKey("test-api-key"));
        await umaClient.GetHeadAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task can_connect_with_well_known_ca_and_api_key()
    {
        var caPath = Path.Combine(AppContext.BaseDirectory, "certs", "ca.pem");
        if (!File.Exists(caPath))
            throw new InvalidOperationException($"Test CA not found at {caPath}. Ensure certs are copied to output.");

        using var caCert = X509CertificateLoader.LoadCertificateFromFile(caPath);
        using (var store = new X509Store(StoreName.Root, StoreLocation.CurrentUser))
        {
            store.Open(OpenFlags.ReadWrite);
            store.Add(caCert);
            try
            {
                using var umaClient = UmaClient.Connect(new UmaClientOptions()
                    .WithHost("localhost")
                    .WithPort(50001)
                    .WithApiKey("test-api-key")
                    .EnableTls());
                await umaClient.GetHeadAsync(TestContext.Current.CancellationToken);
            }
            finally
            {
                store.Remove(caCert);
            }
        }
    }
}