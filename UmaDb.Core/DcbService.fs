namespace UmaDb.Core

open System.Collections.Generic
open System.ServiceModel
open System.Threading.Tasks
open ProtoBuf.Grpc

[<ServiceContract(Name = "umadb.v1.DCB")>]
type IDcbService =
    [<OperationContract>]
    abstract Read : ReadRequest * CallContext -> IAsyncEnumerable<ReadResponse>

    [<OperationContract>]
    abstract Append : AppendRequest * CallContext -> ValueTask<AppendResponse>

    [<OperationContract>]
    abstract Head : HeadRequest * CallContext -> ValueTask<HeadResponse>

    [<OperationContract>]
    abstract GetTrackingInfo : TrackingRequest * CallContext -> ValueTask<TrackingResponse>
