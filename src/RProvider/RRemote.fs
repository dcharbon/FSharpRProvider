namespace RProvider

open Microsoft.FSharp.Core.CompilerServices
open RDotNet
open RInterop
open Samples.FSharp.ProvidedTypes
open System

type public RRemoteSession(connectionName) =
    
    static member Connect connectionName host port blocking =
        let require = RInteropInternal.eval("require(svSocket)")
        let blockingString =
            match blocking with
            | true -> "TRUE"
            | false -> "FALSE"
        let connectionExpr =
            sprintf "socketConnection(host=\"%s\",port=\"%d\",blocking=%s)" host port blockingString
        RInteropInternal.evalTo connectionExpr connectionName

    static member GetConnection host port blocking =
        let suffix = System.Random().Next()
        let connectionName = sprintf "con%d" suffix
        RRemoteSession.Connect connectionName host port blocking
        new RRemoteSession(connectionName)

    member this.connectionName = connectionName

    member this.eval expr =
        RInteropInternal.eval(sprintf "evalServer(%s,\"%s\")" this.connectionName expr)

    member this.evalTo expr symbol =
        this.eval(sprintf "%s <- %s" symbol expr)
