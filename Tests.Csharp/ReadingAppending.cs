using System.Text.Json;
using UmaDb.Client;
using UmaDb.Client.Messages;
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
        using var umaClient = UmaClient.Connect(new UmaClientOptions().WithHost("localhost").WithPort(50051));
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
        var query = UmaQuery.Where([nameof(OrderCreated)], [$"order-{orderCreated.OrderId}"]).WithOptions(o => o.Limit = 1);
        var (events, _) = await umaClient.ReadListAsync(query, TestContext.Current.CancellationToken);

        var payload = JsonSerializer.Deserialize<OrderCreated>(events[0].Event.Data.ToArray());
        Assert.Single(events);
        Assert.Equal(orderCreated, payload);
    }


    [Fact]
    public async Task can_get_head_position()
    {
        using var umaClient = UmaClient.Connect(new UmaClientOptions().WithHost("localhost").WithPort(50051));
        var head = await umaClient.GetHeadAsync(TestContext.Current.CancellationToken);
        Assert.NotNull(head);
    }

    [Fact]
    public async Task append_returns_commit_position()
    {
        using var umaClient = UmaClient.Connect(new UmaClientOptions().WithHost("localhost").WithPort(50051));
        var evt = new UmaEvent(nameof(OrderCreated), ReadOnlyMemory<byte>.Empty, [$"pos-{Guid.NewGuid()}"]);
        var response = await umaClient.AppendAsync([evt], ct: TestContext.Current.CancellationToken);
        Assert.True(response.Position > 0);
    }

    [Fact]
    public async Task idempotent_append_with_same_id_returns_same_commit_position()
    {
        using var umaClient = UmaClient.Connect(new UmaClientOptions().WithHost("localhost").WithPort(50051));
        var tag = $"idem-{Guid.NewGuid()}";
        var query = UmaQuery.Where([nameof(OrderCreated)], [tag]);
        var id = Guid.NewGuid();
        var evt = new UmaEvent(nameof(OrderCreated), new ReadOnlyMemory<byte>([1, 2, 3]), [tag], id);
        var r1 = await umaClient.AppendAsync([evt], failIfMatch: query, ct: TestContext.Current.CancellationToken);
        var r2 = await umaClient.AppendAsync([evt], failIfMatch: query, ct: TestContext.Current.CancellationToken);
        Assert.Equal(r1.Position, r2.Position);
    }

    [Fact]
    public async Task can_read_backwards()
    {
        using var umaClient = UmaClient.Connect(new UmaClientOptions().WithHost("localhost").WithPort(50051));
        var tag = $"back-{Guid.NewGuid()}";
        await umaClient.AppendAsync([
            new UmaEvent("A", new ReadOnlyMemory<byte>([1]), [tag]),
            new UmaEvent("B", new ReadOnlyMemory<byte>([2]), [tag]),
            new UmaEvent("C", new ReadOnlyMemory<byte>([3]), [tag]),
        ], ct: TestContext.Current.CancellationToken);
        var query = UmaQuery.Where(["A", "B", "C"], [tag]).WithOptions(o => { o.Backwards = true; o.Limit = 2; });
        var (events, _) = await umaClient.ReadListAsync(query, TestContext.Current.CancellationToken);
        Assert.Equal(2, events.Count);
        Assert.Equal("C", events[0].Event.EventType);
        Assert.Equal("B", events[1].Event.EventType);
    }

    [Fact]
    public async Task consistency_boundary_read_then_append_with_condition_after_head()
    {
        using var umaClient = UmaClient.Connect(new UmaClientOptions().WithHost("localhost").WithPort(50051));
        var tag = $"cb-{Guid.NewGuid()}";
        var query = UmaQuery.Where([nameof(OrderCreated)], [tag]);
        var evt1 = new UmaEvent(nameof(OrderCreated), JsonSerializer.SerializeToUtf8Bytes(new OrderCreated(Guid.NewGuid(), 1m)), [tag]);
        await umaClient.AppendAsync([evt1], failIfMatch: query, after: null, ct: TestContext.Current.CancellationToken);
        await foreach (var _ in umaClient.ReadAsync(query.WithOptions(o => { }), TestContext.Current.CancellationToken))
            { }
        var after = await umaClient.GetHeadAsync(TestContext.Current.CancellationToken);
        var evt2 = new UmaEvent(nameof(OrderCreated), JsonSerializer.SerializeToUtf8Bytes(new OrderCreated(Guid.NewGuid(), 2m)), [tag]);
        await umaClient.AppendAsync([evt2], failIfMatch: query, after: after, ct: TestContext.Current.CancellationToken);
        var (events, _) = await umaClient.ReadListAsync(query, TestContext.Current.CancellationToken);
        Assert.Equal(2, events.Count);
    }
    
    [Fact]
    public async Task when_appending_events_conditionally_and_condition_fails_then_IntegrityException_is_thrown()
    {
        using var umaClient = UmaClient.Connect(new UmaClientOptions().WithHost("localhost").WithPort(50051));
        var evt1 = new OrderCreated(Guid.NewGuid(), 100m);
        var evt2 = new OrderCreated(Guid.NewGuid(), 100m);
        var query = UmaQuery.Where(types: [nameof(OrderCreated)], tags: [$"order-{evt1.OrderId}"]);
        var umaEvt1 = new UmaEvent(
            nameof(OrderCreated),
            JsonSerializer.SerializeToUtf8Bytes(evt1),
            [$"order-{evt1.OrderId}"]);
        var umaEvt2 = new UmaEvent(
            nameof(OrderCreated),
            JsonSerializer.SerializeToUtf8Bytes(evt2),
            [$"order-{evt2.OrderId}"]);
        
        await umaClient.AppendAsync(events: [umaEvt1], failIfMatch: query, ct: TestContext.Current.CancellationToken);
        
        var exception = await Assert.ThrowsAsync<UmaDbException.IntegrityException>(
            () => umaClient.AppendAsync(events: [umaEvt2], failIfMatch: query, ct: TestContext.Current.CancellationToken).AsTask());
        
        Assert.IsAssignableFrom<UmaDbException>(exception);
    }
}