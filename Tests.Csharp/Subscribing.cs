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
        using var umaClient = UmaClient.Connect("localhost", 50051);
        var tag = $"subscribe-test-{Guid.NewGuid()}";
        var orderCreated = new OrderCreated(Guid.NewGuid(), 42m);
        var eventToAppend = new UmaEvent(
            nameof(OrderCreated),
            JsonSerializer.SerializeToUtf8Bytes(orderCreated),
            [tag]);

        var promise = new TaskCompletionSource<SequencedUmaEvent>();
        var ct = TestContext.Current.CancellationToken;

        void OnEvent(SequencedUmaEvent evt)
        {
            if (evt.Event.Tags?.Contains(tag) == true)
                promise.TrySetResult(evt);
        }

        using var subscription = umaClient.Subscribe(
            UmaFilter.Where(types: [nameof(OrderCreated)], tags: [tag]),
            OnEvent,
            ct);

        await umaClient.AppendAsync([eventToAppend], ct: ct);

        var received = await promise.Task;
        Assert.Equal(nameof(OrderCreated), received.Event.EventType);
        Assert.Contains(tag, received.Event.Tags ?? []);
        Assert.Equal(orderCreated, JsonSerializer.Deserialize<OrderCreated>(received.Event.Data.ToArray()));
    }

    [Fact]
    public async Task can_subscribe_to_events_using_read_async()
    {
        using var umaClient = UmaClient.Connect("localhost", 50051);
        var tag = $"subscribe-expert-{Guid.NewGuid()}";
        var orderCreated = new OrderCreated(Guid.NewGuid(), 99m);
        var eventToAppend = new UmaEvent(
            nameof(OrderCreated),
            JsonSerializer.SerializeToUtf8Bytes(orderCreated),
            [tag]);

        var promise = new TaskCompletionSource<SequencedUmaEvent>();
        var ct = TestContext.Current.CancellationToken;

        void OnEvent(SequencedUmaEvent evt)
        {
            if (evt.Event.Tags?.Contains(tag) == true)
                promise.TrySetResult(evt);
        }

        var subscription = umaClient.ReadAsync(
            UmaFilter.Where(types: [nameof(OrderCreated)], tags: [tag]).WithOptions(o => o.Subscribe = true),
            ct);

        _ = Task.Run(async () =>
        {
            await foreach (var batch in subscription)
            {
                foreach (var evt in batch.Events)
                    OnEvent(evt);
            }
        }, ct);

        await umaClient.AppendAsync([eventToAppend], ct: ct);

        var received = await promise.Task;
        Assert.Equal(nameof(OrderCreated), received.Event.EventType);
        Assert.Contains(tag, received.Event.Tags ?? []);
        Assert.Equal(orderCreated, JsonSerializer.Deserialize<OrderCreated>(received.Event.Data.ToArray()));
    }
}
