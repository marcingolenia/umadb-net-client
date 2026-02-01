namespace Microsoft.Extensions.DependencyInjection

open System.Runtime.CompilerServices
open Client
open UmaClient
open Microsoft.Extensions.Configuration

[<CLIMutable>]
type UmaDbOptions = {
    Host: string
    Port: int
    CaCertPath: string
    ApiKey: string
}
with
    static member Default = { Host = "localhost"; Port = 15001; CaCertPath = null; ApiKey = null }

[<Extension>]
type UmaDbClientExtensions =

    [<Extension>]
    static member AddUmaDbClient(services: IServiceCollection, config: IConfiguration) =
        let options =
            config.GetSection("UmaDb").Get<UmaDbOptions>()
            |> Option.ofObj
            |> Option.defaultValue UmaDbOptions.Default
        UmaDbClientExtensions.AddUmaDbClient(services, options)

    [<Extension>]
    static member AddUmaDbClient(services: IServiceCollection, options: UmaDbOptions) =
        services.AddSingleton<UmaClient>(fun _ ->
            UmaClient.Connect(options.Host, options.Port, options.CaCertPath, options.ApiKey))
        |> ignore
        services
