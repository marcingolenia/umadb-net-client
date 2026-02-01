module Client.UmaClient


open System
open System.Runtime.InteropServices
open System.Threading
open System.Threading.Tasks
open Client.UmaConnection
open Grpc.Core
open ProtoBuf.Grpc.Client
open UmaDb.Core
open ProtoBuf.Grpc

type UmaClient (connection: UmaConnectionResult) =
    let service = connection.GetCallInvoker().CreateGrpcService<IDcbService>()

    let withContext ct =
        let opts = CallOptions(cancellationToken = ct)
        if ct = CancellationToken.None then CallContext.Default else CallContext(&opts)

    member _.AppendAsync(request: AppendRequest) : Task<AppendResponse> =
        let ctx = withContext CancellationToken.None
        service.Append(request, ctx).AsTask()

    member _.AppendAsync(request: AppendRequest, ct: CancellationToken) : Task<AppendResponse> =
        let ctx = withContext ct
        service.Append(request, ctx).AsTask()

    member _.GetHeadAsync() : Task<HeadResponse> =
        let ctx = withContext CancellationToken.None
        service.Head({ _unused = Nullable() }, ctx).AsTask()

    member _.GetHeadAsync(ct: CancellationToken) : Task<HeadResponse> =
        let ctx = withContext ct
        service.Head({ _unused = Nullable() }, ctx).AsTask()

    interface IDisposable with
        member _.Dispose() = (connection :> IDisposable).Dispose()

    static member Connect(host: string, port: int, [<Optional; DefaultParameterValue(null: string)>] caCert: string, [<Optional; DefaultParameterValue(null: string)>] apiKey: string) =
        let opt s = if String.IsNullOrWhiteSpace s then None else Some s
        let conn = UmaConnection.create host port (opt caCert) (opt apiKey)
        new UmaClient(conn)