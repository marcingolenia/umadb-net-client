using Grpc.Core;
using System.Text.Json;
using UmaDb.Csharp;
using UmaDb.Csharp.Messages;
using Xunit;

namespace Tests.Csharp;

public class ExceptionTests
{
    [Fact]
    public async Task when_storing_non_increasing_tracking_info_then_IntegrityException_is_thrown()
    {
        using var umaClient = UmaClient.Connect("localhost", 50051);
        var trackingInfo = new UmaTrackingInfo($"{Guid.NewGuid()}", 20);
        await umaClient.AppendAsync(events: [], trackingInfo: trackingInfo);
        
        var exception = await Assert.ThrowsAsync<UmaDbException.IntegrityException>(
            () => umaClient.AppendAsync(events: [], trackingInfo: trackingInfo).AsTask());
        
        Assert.IsAssignableFrom<UmaDbException>(exception);
    }

    [Fact]
    public async Task when_appending_events_conditionally_and_condition_fails_then_IntegrityException_is_thrown()
    {
        using var umaClient = UmaClient.Connect("localhost", 50051);
        var evt1 = new OrderCreated(Guid.NewGuid(), 100m);
        var evt2 = new OrderCreated(Guid.NewGuid(), 100m);
        var filter = UmaFilter.Where(types: [nameof(OrderCreated)], tags: [$"order-{evt1.OrderId}"]);
        var umaEvt1 = new UmaEvent(
            nameof(OrderCreated),
            JsonSerializer.SerializeToUtf8Bytes(evt1),
            [$"order-{evt1.OrderId}"]);
        var umaEvt2 = new UmaEvent(
            nameof(OrderCreated),
            JsonSerializer.SerializeToUtf8Bytes(evt2),
            [$"order-{evt2.OrderId}"]);
        
        await umaClient.AppendAsync(events: [umaEvt1], failIfMatch: filter);
        
        var exception = await Assert.ThrowsAsync<UmaDbException.IntegrityException>(
            () => umaClient.AppendAsync(events: [umaEvt2], failIfMatch: filter).AsTask());
        
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

    [Fact(Skip = "Requires server with authentication enabled")]
    public async Task when_using_invalid_api_key_then_AuthenticationException_is_thrown()
    {
        using var umaClient = UmaClient.Connect("localhost", 50051, caCert: null, apiKey: "invalid-key");
        await Assert.ThrowsAsync<UmaDbException.AuthenticationException>(
            () => umaClient.GetHeadAsync().AsTask());
    }

    [Fact(Skip = "Requires server with authentication enabled and TLS")]
    public async Task when_using_invalid_api_key_with_tls_then_AuthenticationException_is_thrown()
    {
        using var umaClient = UmaClient.Connect("localhost", 50051, caCert: "~/code/key.pem", apiKey: "invalid-key");
        await Assert.ThrowsAsync<UmaDbException.AuthenticationException>(
            () => umaClient.GetHeadAsync().AsTask());
    }
}
