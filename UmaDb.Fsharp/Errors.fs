/// <summary>Exception and error types for UmaDB client failures. Use <c>UmaDbException.ToUmaDbException</c> to map gRPC exceptions.</summary>
module UmaDb.Client.Errors

open System
open Grpc.Core

/// <summary>Base exception for UmaDB client errors. Derived types indicate the gRPC status (auth, integrity, corruption, etc.).</summary>
type UmaDbException(message: string, ?innerException: Exception) =
    inherit Exception(message, defaultArg innerException null)

    /// <summary>Maps a gRPC <c>RpcException</c> to the corresponding <c>UmaDbException</c> derived type.</summary>
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

/// <summary>I/O or connectivity failure.</summary>
and IoException(message: string) =
    inherit UmaDbException(message)

/// <summary>Invalid argument / serialization error (e.g. malformed request).</summary>
and SerializationException(message: string) =
    inherit UmaDbException(message)

/// <summary>Append condition failed or tracking position not strictly increasing. When returned via <c>append</c>, surfaced as <c>Error (ErrorMessage _)</c>.</summary>
and IntegrityException(message: string) =
    inherit UmaDbException(message)

/// <summary>Data loss or corruption reported by the server.</summary>
and CorruptionException(message: string) =
    inherit UmaDbException(message)

/// <summary>Internal server error.</summary>
and InternalException(message: string) =
    inherit UmaDbException(message)

/// <summary>Authentication failed (e.g. missing or invalid API key).</summary>
and AuthenticationException(message: string) =
    inherit UmaDbException(message)

/// <summary>Operation was cancelled (e.g. <c>CancellationToken</c>).</summary>
and CancelledException(message: string, ?innerException: Exception) =
    inherit UmaDbException(message, defaultArg innerException null)

/// <summary>Returned by <c>append</c> when the append condition fails or tracking position is not strictly increasing (instead of throwing).</summary>
type IntegrityError = ErrorMessage of string