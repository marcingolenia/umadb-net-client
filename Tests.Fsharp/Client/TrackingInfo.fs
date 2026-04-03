module Tests.Client.TrackingInfo

open System.Threading
open FsUnit.Xunit
open Xunit
open UmaDb.Client.ClientBuilder
open UmaDb.Client.Errors
open UmaDb.Client.Operations


[<Fact>]
let ``can store tracking info`` () = 
    task {
        // Arrange
        use uma = connect "localhost" 50002 |> build
        let source = "test"
        let! trackingInfo = readTrackingInfo uma CancellationToken.None source
        let next = (trackingInfo |> Option.defaultValue 0L) + 1L
        // Act
        let! appendResponse = append uma CancellationToken.None (appendOperation [] |> track source next)
        // Assert
        appendResponse.IsOk |> should be True
        let! actualTrackingInfo = readTrackingInfo uma CancellationToken.None source
        actualTrackingInfo.Value |> should equal next
    }
    
[<Fact>]
let ``when storing non increasing tracking info then IntegrityException is thrown``() = 
    task {
        // Arrange
        use uma = connect "localhost" 50002 |> build
        let source = "test"
        let! trackingInfo = readTrackingInfo uma CancellationToken.None source
        let sameNext = (trackingInfo |> Option.defaultValue 0L) 
        // Act
        let! appendResponse = append uma CancellationToken.None (appendOperation [] |> track source sameNext)
        // Assert
        match appendResponse with
        | Error (IntegrityError.ErrorMessage msg) -> msg |> should haveSubstring "integrity error: condition failed: non-increasing tracking position for source"
        | _ -> failwith "Expected Error (IntegrityError.ErrorMessage)"
    }