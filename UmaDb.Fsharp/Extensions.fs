module Extensions

open System.Threading.Tasks

type ValueTask<'T> with
    member this.ToTask() : Task<'T> =
        if this.IsCompletedSuccessfully then
            task { return this.Result }
        else
            this.AsTask()