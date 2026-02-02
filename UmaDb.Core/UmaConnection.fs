module Client.UmaConnection

open System
open System.Net.Http
open System.Net.Security
open System.Security.Cryptography.X509Certificates
open Grpc.Core
open Grpc.Core.Interceptors
open Grpc.Net.Client

let private nonEmpty (s: string) = not (String.IsNullOrWhiteSpace s)

let private validPort port = port >= 1 && port <= 65535

let private validateWithCa (caCert: X509Certificate2) (_: obj) (cert: X509Certificate) (_: X509Chain) (errors: SslPolicyErrors) =
    if errors = SslPolicyErrors.None then true
    else
        use chain = new X509Chain()
        chain.ChainPolicy.ExtraStore.Add(caCert) |> ignore
        chain.ChainPolicy.RevocationMode <- X509RevocationMode.NoCheck
        chain.ChainPolicy.VerificationFlags <- X509VerificationFlags.AllowUnknownCertificateAuthority
        chain.Build(cert :?> X509Certificate2)

/// Connection wrapper that owns the channel and optional CA cert. Dispose to release both.
type UmaConnectionResult internal (channel: GrpcChannel, certToDispose: X509Certificate2 option, apiKey: string option) =
    member _.GetCallInvoker() : CallInvoker =
        match apiKey with
        | Some key ->
            let interceptor = AuthInterceptor(key)
            channel.CreateCallInvoker().Intercept(interceptor)
        | None -> channel.CreateCallInvoker()
    interface IDisposable with
        member _.Dispose() =
            certToDispose |> Option.iter _.Dispose()
            channel.Dispose()

and private AuthInterceptor(apiKey: string) =
    inherit Interceptor()
    let addHeader (ctx: ClientInterceptorContext<_, _>) = ctx.Options.Headers.Add("x-api-key", apiKey)
    let withHeader context f = addHeader context; f ()
    override _.AsyncUnaryCall(request, context, continuation) =
        withHeader context (fun () -> continuation.Invoke(request, context))
    override _.AsyncServerStreamingCall(request, context, continuation) =
        withHeader context (fun () -> continuation.Invoke(request, context))
    override _.AsyncClientStreamingCall(context, continuation) =
        withHeader context (fun () -> continuation.Invoke(context))
    override _.AsyncDuplexStreamingCall(context, continuation) =
        withHeader context (fun () -> continuation.Invoke(context))
    override _.BlockingUnaryCall(request, context, continuation) =
        withHeader context (fun () -> continuation.Invoke(request, context))

module UmaConnection =
    let create (host: string) (port: int) (caCertPath: string option) (apiKey: string option) : UmaConnectionResult =
        if not (nonEmpty host) then invalidArg "host" "Host cannot be empty."
        if not (validPort port) then invalidArg "port" "Port must be between 1 and 65535."
        match apiKey, caCertPath with
        | Some _, None -> invalidArg "apiKey" "Security Risk: API Key cannot be sent over unencrypted connections (missing CA Cert)."
        | _ -> ()
        let address =
            match caCertPath with
            | Some _ -> $"https://{host}:{port}"
            | None -> $"http://{host}:{port}"
        let handler = new SocketsHttpHandler(
            // Prevents "Stuck" connections if the network drops
            PooledConnectionIdleTimeout = TimeSpan.FromMinutes(2.0),
            //  Allows the client to open new sockets if the current HTTP/2 stream limit is reached (usually 100).
            EnableMultipleHttp2Connections = true 
        )
        let certToDispose =
            match caCertPath with
            | Some path ->
                let caCert = X509CertificateLoader.LoadCertificateFromFile(path)
                handler.SslOptions <- SslClientAuthenticationOptions(
                    RemoteCertificateValidationCallback = RemoteCertificateValidationCallback(validateWithCa caCert))
                Some caCert
            | None ->
                handler.SslOptions <- null
                None
        let options = GrpcChannelOptions(HttpHandler = handler, DisposeHttpClient = true)
        let channel = GrpcChannel.ForAddress(address, options)
        new UmaConnectionResult(channel, certToDispose, apiKey)
