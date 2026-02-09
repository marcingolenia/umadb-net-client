using Grpc.Core;
using UmaDb.Client;
using UmaDb.Client.Messages;
using Xunit;

namespace Tests.Csharp;

public class RcpExceptionTests
{
    [Fact]
    public async Task when_storing_non_increasing_tracking_info_then_IntegrityException_is_thrown()
    {
        using var umaClient = UmaClient.Connect(new UmaClientOptions().WithHost("localhost").WithPort(50051));
        var trackingInfo = new UmaTrackingInfo($"{Guid.NewGuid()}", 20);
        await umaClient.AppendAsync(events: [], trackingInfo: trackingInfo, ct: TestContext.Current.CancellationToken);
        
        var exception = await Assert.ThrowsAsync<UmaDbException.IntegrityException>(
            () => umaClient.AppendAsync(events: [], trackingInfo: trackingInfo, ct: TestContext.Current.CancellationToken).AsTask());
        Assert.IsAssignableFrom<UmaDbException>(exception);
    }

    [Theory]
    [InlineData(StatusCode.Unauthenticated, typeof(UmaDbException.AuthenticationException), "Invalid credentials")]
    [InlineData(StatusCode.InvalidArgument, typeof(UmaDbException.SerializationException), "Invalid query format")]
    [InlineData(StatusCode.DataLoss, typeof(UmaDbException.CorruptionException), "Data corruption detected")]
    [InlineData(StatusCode.Internal, typeof(UmaDbException.InternalException), "Internal server error")]
    [InlineData(StatusCode.FailedPrecondition, typeof(UmaDbException.IntegrityException), "Precondition failed")]
    public void when_rpc_exception_has_status_code_then_correct_exception_type_is_returned(
        StatusCode statusCode, Type expectedType, string message)
    {
        var rpcException = new RpcException(new Status(statusCode, message));
        var exception = UmaDbException.ToUmaDbException(rpcException);
        
        Assert.IsType(expectedType, exception);
        Assert.Equal(message, exception.Message);
    }

    [Theory]
    [InlineData(StatusCode.Unknown, "Unknown error")]
    [InlineData(StatusCode.Unavailable, "Service unavailable")]
    public void when_rpc_exception_is_unmapped_status_then_generic_UmaDbException_is_returned(
        StatusCode statusCode, string message)
    {
        var rpcException = new RpcException(new Status(statusCode, message));
        var exception = UmaDbException.ToUmaDbException(rpcException);

        Assert.IsType<UmaDbException>(exception);
        Assert.Contains($"gRPC error: {message}", exception.Message);
        Assert.Equal(rpcException, exception.InnerException);
    }
}
