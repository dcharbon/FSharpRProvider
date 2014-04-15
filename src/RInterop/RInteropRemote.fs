module RInterop.Remote

open Microsoft.FSharp.Reflection
open RDotNet
open RInterop
open RInterop.RInteropInternal
open System
open System.Collections.Generic
open System.Reflection

type RemoteSymbolicExpression(getValue: RemoteSymbolicExpression -> SymbolicExpression, name) =
    member this.name = name
    member this.GetValue () =
        getValue(this)

type RemoteSession(connectionName) =
    static member GetConnection(?host, ?port, ?blocking) =
        let host = defaultArg host "localhost"
        let port = defaultArg port 8888
        let blocking =
            match blocking with
            | Some true -> "TRUE"
            | _ -> "FALSE"
        let connectionName = getNextSymbolName()
        eval("require(svSocket)") |> ignore // FIXME do something if this fails
        eval(sprintf "%s <- socketConnection(host='%s', port='%d', blocking='%s')" connectionName host port blocking) |> ignore
        new RemoteSession(connectionName)

    member this.connectionName = connectionName

    member this.evalToSymbolicExpr expr =
        eval(sprintf "evalServer(%s, \"%s\")" connectionName expr)

    member this.getHandleValue (handle: RemoteSymbolicExpression) =
        this.evalToSymbolicExpr(handle.name)

    member this.evalToHandle expr =
        let handleName = getNextSymbolName()
        eval(sprintf "%s <- evalServer(%s, \"%s\"); TRUE" handleName connectionName expr) |> ignore
        new RemoteSymbolicExpression(this.getHandleValue, handleName)

    member this.call (packageName: string) (funcName: string) (serializedRVal:string) (namedArgs: obj[]) (varArgs: obj[]) : RemoteSymbolicExpression =
        call_ this.evalToHandle packageName funcName serializedRVal namedArgs varArgs

    member this.callFunc (packageName: string) (funcName: string) (argsByName: seq<KeyValuePair<string, obj>>) (varArgs: obj[]) : RemoteSymbolicExpression =
        callFunc_ this.evalToHandle packageName funcName argsByName varArgs