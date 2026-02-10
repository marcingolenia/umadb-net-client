using Grpc.Core;

namespace UmaDb.Client;

/// <summary>
/// Base exception class for UmaDb-related errors.
/// </summary>
public class UmaDbException : Exception
{
    public UmaDbException(string message) : base(message) { }

    public UmaDbException(string message, Exception innerException) : base(message, innerException) { }

    /// <summary>
    /// Converts a gRPC <see cref="RpcException"/> to an appropriate <see cref="UmaDbException"/> based on the status code.
    /// </summary>
    public static UmaDbException ToUmaDbException(RpcException rpcException)
    {
        var errorMessage = rpcException.Status.Detail;
        return rpcException.Status.StatusCode switch
        {
            StatusCode.Unauthenticated => new AuthenticationException(errorMessage),
            StatusCode.FailedPrecondition => new IntegrityException(errorMessage),
            StatusCode.DataLoss => new CorruptionException(errorMessage),
            StatusCode.InvalidArgument => new SerializationException(errorMessage),
            StatusCode.Internal => new InternalException(errorMessage),
            StatusCode.Cancelled => new CancelledException(errorMessage, rpcException),
            _ => new UmaDbException($"gRPC error: {errorMessage}", rpcException)
        };
    }

    /// <summary>
    /// Indicates an I/O-related error, such as network failures or inability to reach the UmaDb server.
    /// </summary>
    public sealed class IoException : UmaDbException
    {
        public IoException(string message) : base(message) { }
    }

    /// <summary>
    /// Indicates a failure during serialization or deserialization of events, queries, or responses.
    /// </summary>
    public sealed class SerializationException : UmaDbException
    {
        public SerializationException(string message) : base(message) { }
    }

    /// <summary>
    /// Indicates an integrity violation, such as appending events that violate constraints or failing conditional operations.
    /// </summary>
    public sealed class IntegrityException : UmaDbException
    {
        public IntegrityException(string message) : base(message) { }
    }

    /// <summary>
    /// Indicates corruption detected in persisted data, such as invalid event format or inconsistent state.
    /// </summary>
    public sealed class CorruptionException : UmaDbException
    {
        public CorruptionException(string message) : base(message) { }
    }

    /// <summary>
    /// Represents an internal server or client error that does not fall into other specific categories.
    /// </summary>
    public sealed class InternalException : UmaDbException
    {
        public InternalException(string message) : base(message) { }
    }

    /// <summary>
    /// Indicates an authentication failure, such as invalid credentials or lack of authorization to access the requested resource.
    /// </summary>
    public sealed class AuthenticationException : UmaDbException
    {
        public AuthenticationException(string message) : base(message) { }
    }

    /// <summary>
    /// Raised when a gRPC call is cancelled (e.g. <see cref="System.Threading.CancellationToken"/> cancelled during a stream).
    /// </summary>
    public sealed class CancelledException : UmaDbException
    {
        public CancelledException(string message) : base(message) { }

        public CancelledException(string message, Exception innerException) : base(message, innerException) { }
    }
}