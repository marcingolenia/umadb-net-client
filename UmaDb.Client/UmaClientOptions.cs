namespace UmaDb.Client;

/// <summary>Fluent options for connecting to Uma DB. Use with <see cref="UmaClient.Connect(UmaClientOptions)"/>.</summary>
public sealed class UmaClientOptions
{
    /// <summary>Server hostname (e.g. "localhost" or "db.example.com").</summary>
    public string? Host { get; private set; }

    /// <summary>Server port (1–65535). Defaults to 50051.</summary>
    public int Port { get; private set; } = 50051;

    /// <summary>API key for authorization. When set, TLS is used automatically.</summary>
    public string? ApiKey { get; private set; }

    /// <summary>Path to server CA or server certificate (PEM). Use for self-signed or custom CA.</summary>
    public string? CaCertPath { get; private set; }

    /// <summary>Use TLS with system trust (well-known CAs).</summary>
    public bool UseTls { get; private set; }

    /// <summary>Server hostname (e.g. "localhost" or "db.example.com").</summary>
    public UmaClientOptions WithHost(string host)
    {
        Host = host;
        return this;
    }

    /// <summary>Server port (1–65535). Defaults to 50051.</summary>
    public UmaClientOptions WithPort(int port)
    {
        Port = port;
        return this;
    }

    /// <summary>API key for authorization. When set, TLS is used automatically (key is never sent over plain HTTP).</summary>
    public UmaClientOptions WithApiKey(string? apiKey)
    {
        ApiKey = apiKey;
        return this;
    }

    /// <summary>Path to server CA or server certificate (PEM). Use when the server uses a self-signed or custom CA. Implies TLS.</summary>
    public UmaClientOptions WithCaCert(string? path)
    {
        CaCertPath = path;
        return this;
    }

    /// <summary>Use TLS with system trust (well-known CAs). Use when the server has a certificate from a public CA.</summary>
    public UmaClientOptions EnableTls()
    {
        UseTls = true;
        return this;
    }
}
