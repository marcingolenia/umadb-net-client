module Extensions

open System.Threading.Tasks

type ValueTask<'T> with
    member this.ToAsync() : Async<'T> =
        if this.IsCompletedSuccessfully then
            async { return this.Result }
        else
            this.AsTask() |> Async.AwaitTask