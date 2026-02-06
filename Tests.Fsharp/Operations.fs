module Operations

open System
open FsUnit.Xunit
open UmaDb.Core
open Xunit

let event =
    { EventType = "TestEvent"
      Tags = ResizeArray([ "operations"; "operations-2" ])
      Data = System.Text.Encoding.UTF8.GetBytes("Hello UmaDB")
      Uuid = Guid.NewGuid().ToString() }

let appendRequest =
    { Events = ResizeArray([ event ])
      Condition = Nullable.appendCondition
      TrackingInfo = Nullable.trackingInfo }

