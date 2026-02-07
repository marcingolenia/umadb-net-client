using System.Text.Json;
using UmaDb.Csharp;
using UmaDb.Csharp.Messages;
using Xunit;

namespace Tests.Csharp;

public class Subscribing
{
    [Fact]
    public async Task can_subscribe_to_events()
    {
        using var umaClient = UmaClient.Connect(new UmaClientOptions().WithHost("localhost").WithPort(50051));
        var tag = $"subscribe-{Guid.NewGuid()}";
        var orderCreated = new OrderCreated(Guid.NewGuid(), 42m);
        var eventToAppend = new UmaEvent(
            nameof(OrderCreated),
            JsonSerializer.SerializeToUtf8Bytes(orderCreated),
            [tag]);

        var received = new TaskCompletionSource<SequencedUmaEvent>();
        var ct = TestContext.Current.CancellationToken;

        using var subscription = umaClient.Subscribe(
            UmaFilter.Where([nameof(OrderCreated)], [tag]),
            evt => received.TrySetResult(evt),
            ct);

        await umaClient.AppendAsync([eventToAppend], ct: ct);

        AssertReceivedOrderCreated(await received.Task, tag, orderCreated);
    }

    [Fact]
    public async Task can_subscribe_to_events_using_read_async()
    {
        using var umaClient = UmaClient.Connect(new UmaClientOptions().WithHost("localhost").WithPort(50051));
        var tag = $"subscribe-{Guid.NewGuid()}";
        var orderCreated = new OrderCreated(Guid.NewGuid(), 99m);
        var eventToAppend = new UmaEvent(
            nameof(OrderCreated),
            JsonSerializer.SerializeToUtf8Bytes(orderCreated),
            [tag]);

        var received = new TaskCompletionSource<SequencedUmaEvent>();
        var ct = TestContext.Current.CancellationToken;

        var subscription = umaClient.ReadAsync(
            UmaFilter.Where([nameof(OrderCreated)], [tag]).WithOptions(o => o.Subscribe = true),
            ct);

        _ = Task.Run(async () =>
        {
            await foreach (var batch in subscription)
            {
                foreach (var evt in batch.Events)
                    received.TrySetResult(evt);
            }
        }, ct);

        await umaClient.AppendAsync([eventToAppend], ct: ct);

        AssertReceivedOrderCreated(await received.Task, tag, orderCreated);
    }

    static void AssertReceivedOrderCreated(SequencedUmaEvent received, string tag, OrderCreated expected)
    {
        Assert.Equal(nameof(OrderCreated), received.Event.EventType);
        Assert.Contains(tag, received.Event.Tags ?? []);
        Assert.Equal(expected, JsonSerializer.Deserialize<OrderCreated>(received.Event.Data.ToArray()));
    }
}
