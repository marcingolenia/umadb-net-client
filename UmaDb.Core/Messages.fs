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
    { [<ProtoMember(1)>] FailIfEventsMatch: Query
      [<ProtoMember(2)>] After: Nullable<uint64> }

[<ProtoContract; CLIMutable>]
type ReadRequest =
    { [<ProtoMember(1)>] Query: Query
      [<ProtoMember(2)>] Start: Nullable<uint64>
      [<ProtoMember(3)>] Backwards: bool
      [<ProtoMember(4)>] Limit: Nullable<uint32>
      [<ProtoMember(6)>] BatchSize: Nullable<uint32> }

[<ProtoContract; CLIMutable>]
type ReadResponse =
    { [<ProtoMember(1)>] Events: ResizeArray<SequencedEvent>
      [<ProtoMember(2)>] Head: Nullable<uint64> }

[<ProtoContract; CLIMutable>]
type SubscribeRequest =
    { [<ProtoMember(1)>] Query: Query
      [<ProtoMember(2)>] After: Nullable<uint64>
      [<ProtoMember(3)>] BatchSize: Nullable<uint32> }

[<ProtoContract; CLIMutable>]
type SubscribeResponse =
    { [<ProtoMember(1)>] Events: ResizeArray<SequencedEvent> }

[<ProtoContract; CLIMutable>]
type TrackingInfo =
    { [<ProtoMember(1)>] Source: string
      [<ProtoMember(2)>] Position: uint64 }

[<ProtoContract; CLIMutable>]
type AppendRequest =
    { [<ProtoMember(1)>] Events: ResizeArray<Event>
      [<ProtoMember(2)>] Condition: AppendCondition
      [<ProtoMember(3)>] TrackingInfo: TrackingInfo }

/// Nullable (null) values for optional message types. Use when a Condition or Query is not present.
/// F# records cannot use the null literal; these values serialize as absent in protobuf.
module Nullable =
    let appendCondition: AppendCondition = Unchecked.defaultof<AppendCondition>
    let query: Query = Unchecked.defaultof<Query>
    let trackingInfo: TrackingInfo = Unchecked.defaultof<TrackingInfo>

[<ProtoContract; CLIMutable>]
type AppendResponse =
    { [<ProtoMember(1)>] Position: uint64 }

[<ProtoContract; CLIMutable>]
type HeadRequest =
    { [<ProtoMember(1)>] _unused: Nullable<int> }

[<ProtoContract; CLIMutable>]
type HeadResponse =
    { [<ProtoMember(1)>] Position: Nullable<uint64> }

[<ProtoContract; CLIMutable>]
type TrackingRequest =
    { [<ProtoMember(1)>] Source: string }

[<ProtoContract; CLIMutable>]
type TrackingResponse =
    { [<ProtoMember(1)>] Position: Nullable<uint64> }

[<ProtoContract>]
type ErrorType =
    | IO = 0
    | Serialization = 1
    | Integrity = 2
    | Corruption = 3
    | Internal = 4
    | Authentication = 5
    | InvalidArgument = 6

[<ProtoContract; CLIMutable>]
type ErrorResponse =
    { [<ProtoMember(1)>] Message: string
      [<ProtoMember(2)>] ErrorType: ErrorType }