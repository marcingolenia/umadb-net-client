using System.Text.Json;
using UmaDb.Csharp;
using UmaDb.Csharp.Messages;
using Xunit;

namespace Tests.Csharp;

// Records are great for events, because they are immutable by default
public record OrderCreated(Guid OrderId, decimal Amount);

public record OrderShipped(Guid OrderId, string Address);

public class ReadingAppending
{
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

        await umaClient.AppendAsync([evt1, evt2], ct: TestContext.Current.CancellationToken);
        var query = UmaFilter.Where([nameof(OrderCreated)], [$"order-{orderCreated.OrderId}"]).WithOptions(o => o.Limit = 1);
        var events = await umaClient.ReadListAsync(query, TestContext.Current.CancellationToken);

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
        await umaClient.AppendAsync([evt1, evt1, evt1, evt1, evt1, evt1, evt1, evt1, evt1, evt1, evt1], ct: TestContext.Current.CancellationToken);
        var events = umaClient.ReadAsync(UmaFilter.All.WithOptions(o => o.BatchSize = 5), TestContext.Current.CancellationToken);
        List<UmaReadBatch> batches = [];
        await foreach (var batch in events) batches.Add(batch);
        Assert.True(batches.Count > 1);
        Assert.Equal(5, batches.First().Events.Count);
    }

    [Fact]
    public async Task can_get_head_position()
    {
        using var umaClient = UmaClient.Connect("localhost", 50051);
        var head = await umaClient.GetHeadAsync(TestContext.Current.CancellationToken);
        Assert.NotNull(head);
    }

    [Fact]
    public async Task can_store_tracking_info()
    {
        using var umaClient = UmaClient.Connect("localhost", 50051);
        var expectedTrackingInfo = new UmaTrackingInfo($"{Guid.NewGuid()}", 20);
        await umaClient.AppendAsync(events: [], trackingInfo: expectedTrackingInfo, ct: TestContext.Current.CancellationToken);
        var actualPosition = await umaClient.GetTrackingInfoAsync(expectedTrackingInfo.Source, TestContext.Current.CancellationToken);
        Assert.Equal(actualPosition, expectedTrackingInfo.Position);
    }
}