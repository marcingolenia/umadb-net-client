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
    public void create_uma_client()
    {
        using var umaClient = UmaClient.Connect("localhost", 50051);
    }

    [Fact(Skip = "Requires a key.pem file")]
    public void create_uma_client_with_auth()
    {
        using var umaClient = UmaClient.Connect("localhost", 50051, "~/code/key.pem", "my-api-key");
    }

    [Fact]
    public async Task append_and_read_events()
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
        var query = UmaQuery.Where(types: [nameof(OrderCreated)], tags: [$"order-{orderCreated.OrderId}"]);
        var events = await umaClient.ReadListAsync(query);
        var payload = JsonSerializer.Deserialize<OrderCreated>(events[0].Event.Data.ToArray());
        Assert.Single(events);
        Assert.Equal(orderCreated, payload);
    }

    [Fact]
    public async Task can_read_all_events()
    {
        using var umaClient = UmaClient.Connect("localhost", 50051);
        var orderCreated = new OrderCreated(Guid.NewGuid(), 100m);
        var evt1 = new UmaEvent(
            nameof(OrderCreated),
            JsonSerializer.SerializeToUtf8Bytes(orderCreated),
            [$"order-{orderCreated.OrderId}"]
        );
        await umaClient.AppendAsync([evt1]);
        var events = umaClient.ReadAllAsync();
        List<UmaReadBatch> batches = [];
        await foreach (var batch in events)
        {
            batches.Add(batch);
        }
        Assert.True( batches.Count > 0);
    }
}