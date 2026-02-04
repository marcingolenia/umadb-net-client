using System.Text.Json;
using UmaDb.Csharp;
using UmaDb.Csharp.Messages;
using Xunit;

namespace Tests.Csharp;

// Records are great for events, because they are immutable by default
public record OrderCreated(Guid OrderId, decimal Amount);

public record OrderShipped(Guid OrderId, string Address);

public class Examples
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

    [Fact]
    public async Task can_append_and_read_list_of_events()
    {
        using var umaClient = UmaClient.Connect("localhost", 50051);
        var orderCreated = new OrderCreated(Guid.NewGuid(), 100.32m);
        var orderShipped = new OrderShipped(Guid.NewGuid(), "123 Main St");
        var evt1 = new UmaEvent(
            nameof(OrderCreated),
            JsonSerializer.SerializeToUtf8Bytes(orderCreated),
            [$"order-{orderCreated.OrderId}"]
        );
        var evt2 = new UmaEvent(
            nameof(OrderShipped),
            JsonSerializer.SerializeToUtf8Bytes(orderShipped),
            [$"order-{orderShipped.OrderId}"]
        );

        await umaClient.AppendAsync([evt1, evt2]);
        var query = UmaFilter.Where([nameof(OrderCreated)], [$"order-{orderCreated.OrderId}"]).WithOptions(o => o.Limit = 1);
        var events = await umaClient.ReadListAsync(query);
        
        var payload = JsonSerializer.Deserialize<OrderCreated>(events[0].Event.Data.ToArray());
        Assert.Single(events);
        Assert.Equal(orderCreated, payload);
    }

    [Fact]
    public async Task can_read_all_events_in_batches()
    {
        using var umaClient = UmaClient.Connect("localhost", 50051);
        var orderCreated = new OrderCreated(Guid.NewGuid(), 100m);
        var evt1 = new UmaEvent(
            nameof(OrderCreated),
            JsonSerializer.SerializeToUtf8Bytes(orderCreated),
            [$"order-{orderCreated.OrderId}"]
        );
        await umaClient.AppendAsync([evt1, evt1, evt1, evt1, evt1, evt1, evt1, evt1, evt1, evt1, evt1]);
        var events = umaClient.ReadAsync(UmaFilter.All.WithOptions(o => o.BatchSize = 5));
        List<UmaReadBatch> batches = [];
        await foreach (var batch in events) batches.Add(batch);
        Assert.True(batches.Count > 1);
        Assert.Equal(batches.First().Events.Count, 5);
    }
    
    [Fact]
    public async Task can_get_head_position()
    {
        using var umaClient = UmaClient.Connect("localhost", 50051);
        var head = await umaClient.GetHeadAsync();
        Assert.NotNull(head);
    }

    [Fact]
    public async Task can_store_tracking_info()
    {
        using var umaClient = UmaClient.Connect("localhost", 50051);
        var expectedTrackingInfo = new UmaTrackingInfo($"{Guid.NewGuid()}", 20);
        await umaClient.AppendAsync(events: [], trackingInfo: expectedTrackingInfo);
        var actualPosition = await umaClient.GetTrackingInfoAsync(expectedTrackingInfo.Source);
        Assert.Equal(actualPosition, expectedTrackingInfo.Position);
    }
    
    [Fact]
    public async Task when_storing_non_increasing_then_integrity_error_is_thrown()
    {
        using var umaClient = UmaClient.Connect("localhost", 50051);
        var trackingInfo = new UmaTrackingInfo($"{Guid.NewGuid()}", 20);
        await umaClient.AppendAsync(events: [], trackingInfo: trackingInfo);
        // TODO: Add Custom Exceptions
        var exception = await Assert.ThrowsAsync<Grpc.Core.RpcException>(
            () => umaClient.AppendAsync(events: [], trackingInfo: trackingInfo).AsTask());
        Assert.Equal(Grpc.Core.StatusCode.FailedPrecondition, exception.Status.StatusCode);
    }

    [Fact]
    public async Task when_appending_events_conditionally_and_condition_fails_then_FailedPrecondition_is_thrown()
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
        var exception = await Assert.ThrowsAsync<Grpc.Core.RpcException>(
            () => umaClient.AppendAsync(events: [umaEvt2], failIfMatch: filter).AsTask());
        Assert.Equal(Grpc.Core.StatusCode.FailedPrecondition, exception.Status.StatusCode);
    }
}

// TODO: Subscription
// TODO: Exceptions 