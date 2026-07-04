/// <summary>Builds a connection to UmaDB and creates a <c>UmaClient</c>. Use <c>connect</c>, then pipe <c>withTls</c>, <c>withCaCert</c>, <c>withApiKey</c> as needed, then <c>build</c>.</summary>
module UmaDb.Client.ClientBuilder

open System
open System.Threading
open Client.UmaConnection
open ProtoBuf.Grpc.Client
open ProtoBuf.Grpc
open UmaDb.Core
open UmaDb.Client.Query
open UmaDb.Client.Event

/// <summary>Connection options (host, port, TLS, CA cert, API key). Build with <c>connect</c> and pipeline functions.</summary>
type ClientBuilder =
    { Host: string
      Port: int
      CaCert: string option
      ApiKey: string option
      UseTls: bool }

/// <summary>Starts building connection options (HTTP, no TLS). Pipe to <c>withTls</c>, <c>withCaCert</c>, <c>withApiKey</c> then <c>build</c>.</summary>
let connect (host: string) (port: int): ClientBuilder =
    { Host = host
      Port = port
      CaCert = None
      ApiKey = None
      UseTls = false }

/// <summary>Enables TLS (HTTPS). Use with a server that has a certificate in the system trust store.</summary>
let withTls (builder: ClientBuilder): ClientBuilder =
    { builder with UseTls = true }

/// <summary>Uses a custom CA certificate (e.g. self-signed or private CA). Implies TLS.</summary>
let withCaCert (path: string) (builder: ClientBuilder): ClientBuilder =
    { builder with CaCert = Some path; UseTls = true }

/// <summary>Sets the API key for authentication. Use with TLS so the key is sent over HTTPS.</summary>
let withApiKey (key: string) (builder: ClientBuilder): ClientBuilder =
    { builder with ApiKey = Some key }
    
/// <summary>Client instance for a single UmaDB connection. Dispose when no longer needed.</summary>
type UmaClient(connection: UmaConnectionResult) =
    let service = connection.GetCallInvoker().CreateGrpcService<IDcbService>()
    member internal _.Service = service
    
    member internal _.ReadBatches (query: QueryItem list) (options: QueryOptions) (ct: CancellationToken) =
        let queryProto = Query.toProto query
        let request: ReadRequest =
            { Query = Option.defaultValue Nullable.query queryProto
              Start = toNullableUInt64 options.FromPosition
              Backwards = options.Backwards
              Limit = toNullableUInt32 options.Limit
              BatchSize = toNullableUInt32 options.BatchSize }
        service.Read(request, CallContext.op_Implicit ct)
        
    member internal _.Subscribe (query: QueryItem list) (options: QueryOptions) (ct: CancellationToken) =
        let queryProto = Query.toProto query
        let request: SubscribeRequest =
            { Query = Option.defaultValue Nullable.query queryProto
              After = toNullableUInt64 options.FromPosition
              BatchSize = toNullableUInt32 options.BatchSize }
        service.Subscribe(request, CallContext.op_Implicit ct)

    interface IDisposable with
        member _.Dispose() = (connection :> IDisposable).Dispose()

/// <summary>Creates a <c>UmaClient</c> from the builder. Reuse the instance; dispose when no longer needed.</summary>
let build (builder: ClientBuilder): UmaClient =
    let conn = UmaConnection.create builder.Host builder.Port builder.CaCert builder.ApiKey builder.UseTls
    new UmaClient(conn)
