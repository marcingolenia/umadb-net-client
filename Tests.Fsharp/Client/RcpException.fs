module Tests.Client.RcpException

open FsUnit.Xunit
open Grpc.Core
open Xunit
open UmaDb.Client.Errors

[<Theory>]
[<InlineData(StatusCode.Unauthenticated, "Invalid credentials")>]
[<InlineData(StatusCode.InvalidArgument, "Invalid query format")>]
[<InlineData(StatusCode.DataLoss, "Data corruption detected")>]
[<InlineData(StatusCode.Internal, "Internal server error")>]
[<InlineData(StatusCode.FailedPrecondition, "Precondition failed")>]
let ``when rpc exception has status code then correct exception type is returned``
    (statusCode: StatusCode) (message: string) =
    let rpcException = RpcException(Status(statusCode, message))
    let ex = UmaDbException.ToUmaDbException(rpcException)

    let checkMapped (expected: bool) =
        if not expected then
            failwith $"Expected exception type for {statusCode}, got {ex.GetType().Name}"

    match statusCode with
    | StatusCode.Unauthenticated -> ex :? AuthenticationException |> checkMapped
    | StatusCode.InvalidArgument -> ex :? SerializationException |> checkMapped
    | StatusCode.DataLoss -> ex :? CorruptionException |> checkMapped
    | StatusCode.Internal -> ex :? InternalException |> checkMapped
    | StatusCode.FailedPrecondition -> ex :? IntegrityException |> checkMapped
    | _ -> ()

    ex.Message |> should equal message

[<Theory>]
[<InlineData(StatusCode.Unknown, "Unknown error")>]
[<InlineData(StatusCode.Unavailable, "Service unavailable")>]
let ``when rpc exception is unmapped status then generic UmaDbException is returned``
    (statusCode: StatusCode) (message: string) =
    let rpcException = RpcException(Status(statusCode, message))
    let ex = UmaDbException.ToUmaDbException(rpcException)

    ex.GetType() |> should equal (typeof<UmaDbException>)

    ex.Message |> should haveSubstring $"gRPC error: {message}"
    ex.InnerException |> should equal (box rpcException)
