namespace UmaDb.Core

open System
open ProtoBuf

[<ProtoContract; CLIMutable>]
type Event =
    { [<ProtoMember(1)>] EventType: string
      [<ProtoMember(2)>] Tags: ResizeArray<string>
      [<ProtoMember(3)>] Data: byte[]
      [<ProtoMember(4)>] Uuid: string }

[<ProtoContract; CLIMutable>]
type SequencedEvent =
    { [<ProtoMember(1)>] Position: uint64
      [<ProtoMember(2)>] Event: Event }

[<ProtoContract; CLIMutable>]
type QueryItem =
    { [<ProtoMember(1)>] Types: ResizeArray<string>
      [<ProtoMember(2)>] Tags: ResizeArray<string> }

[<ProtoContract; CLIMutable>]
type Query =
    { [<ProtoMember(1)>] Items: ResizeArray<QueryItem> }

[<ProtoContract; CLIMutable>]
type AppendCondition =
    { [<ProtoMember(1)>] FailIfEventsMatch: Query option
      [<ProtoMember(2)>] After: Nullable<uint64> }

[<ProtoContract; CLIMutable>]
type ReadRequest =
    { [<ProtoMember(1)>] Query: Query option
      [<ProtoMember(2)>] Start: uint64 option
      [<ProtoMember(3)>] Backwards: bool
      [<ProtoMember(4)>] Limit: uint32 option
      [<ProtoMember(5)>] Subscribe: bool
      [<ProtoMember(6)>] BatchSize: uint32 option }

[<ProtoContract; CLIMutable>]
type ReadResponse =
    { [<ProtoMember(1)>] Events: ResizeArray<SequencedEvent>
      [<ProtoMember(2)>] Head: Nullable<uint64> }

[<ProtoContract; CLIMutable>]
type AppendRequest =
    { [<ProtoMember(1)>] Events: ResizeArray<Event>
      [<ProtoMember(2)>] Condition: AppendCondition option }

[<ProtoContract; CLIMutable>]
type AppendResponse =
    { [<ProtoMember(1)>] Position: uint64 }

[<ProtoContract; CLIMutable>]
type HeadRequest =
    { [<ProtoMember(1)>] _unused: Nullable<int> }

[<ProtoContract; CLIMutable>]
type HeadResponse =
    { [<ProtoMember(1)>] Position: Nullable<uint64> }

[<ProtoContract>]
type ErrorType =
    | IO = 0
    | Serialization = 1
    | Integrity = 2
    | Corruption = 3
    | Internal = 4
    | Authentication = 5

[<ProtoContract; CLIMutable>]
type ErrorResponse =
    { [<ProtoMember(1)>] Message: string
      [<ProtoMember(2)>] ErrorType: ErrorType }
