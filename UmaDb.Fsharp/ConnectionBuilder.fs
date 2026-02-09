module UmaDb.Fsharp.ConnectionBuilder

open Client
open Client.UmaConnection

type ConnectionBuilder =
    { Host: string
      Port: int
      CaCert: string option
      ApiKey: string option
      UseTls: bool }

let connect (host: string) (port: int): ConnectionBuilder =
    { Host = host
      Port = port
      CaCert = None
      ApiKey = None
      UseTls = false }

let withTls (builder: ConnectionBuilder): ConnectionBuilder =
    { builder with UseTls = true }

let withCaCert (path: string) (builder: ConnectionBuilder): ConnectionBuilder =
    { builder with CaCert = Some path; UseTls = true }

let withApiKey (key: string) (builder: ConnectionBuilder): ConnectionBuilder =
    { builder with ApiKey = Some key }

let build (builder: ConnectionBuilder): UmaClient =
    let conn = UmaConnection.create builder.Host builder.Port builder.CaCert builder.ApiKey builder.UseTls
    new UmaClient(conn)
