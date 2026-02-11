module Errors

open System
open Grpc.Core

type UmaDbException(message: string, ?innerException: Exception) =
    inherit Exception(message, defaultArg innerException null)

    static member ToUmaDbException(rpcException: RpcException) : UmaDbException =
        let errorMessage = rpcException.Status.Detail
        match rpcException.Status.StatusCode with
        | StatusCode.Unauthenticated -> AuthenticationException(errorMessage) :> UmaDbException
        | StatusCode.FailedPrecondition -> IntegrityException(errorMessage) :> UmaDbException
        | StatusCode.DataLoss -> CorruptionException(errorMessage) :> UmaDbException
        | StatusCode.InvalidArgument -> SerializationException(errorMessage) :> UmaDbException
        | StatusCode.Internal -> InternalException(errorMessage) :> UmaDbException
        | StatusCode.Cancelled -> CancelledException(errorMessage, rpcException) :> UmaDbException
        | _ -> UmaDbException($"gRPC error: {errorMessage}", rpcException)

and IoException(message: string) =
    inherit UmaDbException(message)

and SerializationException(message: string) =
    inherit UmaDbException(message)

and IntegrityException(message: string) =
    inherit UmaDbException(message)

and CorruptionException(message: string) =
    inherit UmaDbException(message)

and InternalException(message: string) =
    inherit UmaDbException(message)

and AuthenticationException(message: string) =
    inherit UmaDbException(message)

and CancelledException(message: string, ?innerException: Exception) =
    inherit UmaDbException(message, defaultArg innerException null)
    
type IntegrityError = ErrorMessage of string