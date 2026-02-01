module SimpleConnectivity

open System
open Grpc.Net.Client
open ProtoBuf.Grpc.Client
open FsUnit.Xunit
open UmaDb.Core
open Xunit

[<Fact>]
let ``Can connect and append event using raw GrpcChannel`` () =
    task {
        AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true)
        use channel = GrpcChannel.ForAddress("http://localhost:50051")
        let client = channel.CreateGrpcService<IDcbService>()
        let event = {
            EventType = "TestEvent"
            Tags = ResizeArray(["test"; "debug"])
            Data = System.Text.Encoding.UTF8.GetBytes("Hello UmaDB")
            Uuid = Guid.NewGuid().ToString()
        }
        let request = {
            Events = ResizeArray([ event ])
            Condition = None
        }
        let! response = client.Append(request, ProtoBuf.Grpc.CallContext.Default)
        response.Position |> should be (greaterThan 0UL)
    }