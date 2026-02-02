module Client.UmaClient

open System
open System.Collections.Generic
open System.Runtime.InteropServices
open System.Threading
open System.Threading.Tasks
open Client.UmaConnection
open FSharp.Control
open ProtoBuf.Grpc.Client
open UmaDb.Core
open ProtoBuf.Grpc

type UmaClient (connection: UmaConnectionResult) =
    let service = connection.GetCallInvoker().CreateGrpcService<IDcbService>()

    member _.AppendAsync(request: AppendRequest) : ValueTask<AppendResponse> =
        service.Append(request, CallContext.op_Implicit CancellationToken.None)

    member _.AppendAsync(request: AppendRequest, ct: CancellationToken) : ValueTask<AppendResponse> =
        // F# compiler chose the wrong constructor overload (CallContext(opts)), skip the drama by using the implicit operator.
        service.Append(request, CallContext.op_Implicit ct)

    member _.GetHeadAsync() : ValueTask<HeadResponse> =
        service.Head({ _unused = Nullable() }, CallContext.op_Implicit CancellationToken.None)

    member _.GetHeadAsync(ct: CancellationToken) : ValueTask<HeadResponse> =
        service.Head({ _unused = Nullable() }, CallContext.op_Implicit ct)

    member _.ReadAsync(request: ReadRequest, ct: CancellationToken) : IAsyncEnumerable<ReadResponse> =
        service.Read(request, CallContext.op_Implicit ct)
        
    member this.ReadListAsync(request: ReadRequest) : Task<List<SequencedEvent>> =
        task {
                let results = List<SequencedEvent>()
                do! this.ReadAsync(request, CancellationToken.None)
                    |> TaskSeq.collectSeq _.Events
                    |> TaskSeq.iter results.Add       
                return results
            }

    member _.ReadAsync(request: ReadRequest) : IAsyncEnumerable<ReadResponse> =
        service.Read(request, CallContext.op_Implicit CancellationToken.None)

    interface IDisposable with
        member _.Dispose() = (connection :> IDisposable).Dispose()

    static member Connect(host: string, port: int, [<Optional; DefaultParameterValue(null: string)>] caCert: string, [<Optional; DefaultParameterValue(null: string)>] apiKey: string) =
        let opt s = if String.IsNullOrWhiteSpace s then None else Some s
        let conn = UmaConnection.create host port (opt caCert) (opt apiKey)
        new UmaClient(conn)