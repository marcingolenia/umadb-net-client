using UmaDb.Csharp;
using UmaDb.Csharp.Messages;
using Xunit;

namespace Tests.Csharp;

public class TrackingInfoTests
{
    [Fact]
    public async Task can_store_tracking_info()
    {
        using var umaClient = UmaClient.Connect(new UmaClientOptions().WithHost("localhost").WithPort(50051));
        var expectedTrackingInfo = new UmaTrackingInfo($"{Guid.NewGuid()}", 20);
        await umaClient.AppendAsync(events: [], trackingInfo: expectedTrackingInfo, ct: TestContext.Current.CancellationToken);
        var actualPosition = await umaClient.GetTrackingInfoAsync(expectedTrackingInfo.Source, TestContext.Current.CancellationToken);
        Assert.Equal(actualPosition, expectedTrackingInfo.Position);
    }

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
}
