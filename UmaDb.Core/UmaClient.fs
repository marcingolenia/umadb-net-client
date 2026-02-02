module Client.UmaClient

open System
open System.Collections.Generic
open System.Runtime.InteropServices
open System.Threading
open System.Threading.Tasks
open Client.UmaConnection
open ProtoBuf.Grpc.Client
open UmaDb.Core
open ProtoBuf.Grpc

type UmaClient (connection: UmaConnectionResult) =
    let service = connection.GetCallInvoker().CreateGrpcService<IDcbService>()

    member _.AppendAsync(request: AppendRequest, [<Optional>] ?ct: CancellationToken) : ValueTask<AppendResponse> =
        let ct = defaultArg ct CancellationToken.None
        // F# compiler chose the wrong constructor overload (CallContext(opts)), skip the drama by using the implicit operator.
        service.Append(request, CallContext.op_Implicit ct)

    member _.GetHeadAsync([<Optional>] ?ct: CancellationToken) : ValueTask<HeadResponse> =
        let ct = defaultArg ct CancellationToken.None
        service.Head({ _unused = Nullable() }, CallContext.op_Implicit ct)

    member _.ReadAsync(request: ReadRequest, [<Optional>] ?ct: CancellationToken) : IAsyncEnumerable<ReadResponse> =
        let ct = defaultArg ct CancellationToken.None
        service.Read(request, CallContext.op_Implicit ct)


    interface IDisposable with
        member _.Dispose() = (connection :> IDisposable).Dispose()

    static member Connect(host: string, port: int, [<Optional; DefaultParameterValue(null: string)>] caCert: string, [<Optional; DefaultParameterValue(null: string)>] apiKey: string) =
        let opt s = if String.IsNullOrWhiteSpace s then None else Some s
        let conn = UmaConnection.create host port (opt caCert) (opt apiKey)
        new UmaClient(conn)