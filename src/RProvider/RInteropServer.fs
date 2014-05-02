﻿namespace RProvider

open Microsoft.FSharp.Core.CompilerServices
open Microsoft.Win32
open RDotNet
open RInterop
open RProvider.Internal
open System

type RInteropServer() =
    inherit MarshalByRefObject()
    
    // Set the R 'R_CStackLimit' variable to -1 when initializing the R engine
    // (the engine is initialized lazily, so the initialization always happens
    // after the static constructor is called - by doing this in the static constructor
    // we make sure that this is *not* set in the normal execution)
    static do RInit.DisableStackChecking <- true
    
    let initResultValue = RInit.initResult.Force()
    let serverLock = "serverLock"
    let withLock f =
        lock serverLock f

    member x.RInitValue
        with get() =
            withLock <| fun () ->
            match initResultValue with
            | RInit.RInitError error -> Some error
            | _ -> None

    member x.GetPackages() =
         withLock <| fun () ->
            RInterop.getPackages()

    member x.LoadPackage(package) =
        withLock <| fun () ->
            RInterop.loadPackage package
        
    member x.GetBindings(package) =
        withLock <| fun () ->
            RInterop.getBindings package
        
    member x.GetFunctionDescriptions(package:string) =
        withLock <| fun () ->
            RInterop.getFunctionDescriptions package
        
    member x.GetPackageDescription(package) =
        withLock <| fun () ->
            RInterop.getPackageDescription package
        
    member x.MakeSafeName(name) =
        withLock <| fun () ->
            makeSafeName name

