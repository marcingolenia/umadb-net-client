module Tests.Client.TrackingInfo

open System.Threading
open FsUnit.Xunit
open Xunit
open UmaDb.Fsharp.ConnectionBuilder
open UmaDb.Fsharp.Client

[<Fact>]
let ``can sore tracking info`` () = 
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
        | Error (Errors.IntegrityError.ErrorMessage msg) ->  msg |> should haveSubstring "Integrity error: condition failed: non-increasing tracking position for source"
        | _ -> failwith "Expected Error (IntegrityError.ErrorMessage)"
    }