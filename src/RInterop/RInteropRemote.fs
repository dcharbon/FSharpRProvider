module RInterop.Remote

open Microsoft.FSharp.Reflection
open RDotNet
open RInterop
open RInterop.Logging
open RInterop.RInteropInternal
open System
open System.Collections.Generic
open System.Reflection

type RemoteSymbolicExpression(getValue: RemoteSymbolicExpression -> SymbolicExpression, name) =
    member this.name = name
    member this.GetValue () =
        getValue(this)

type RemoteSession(connectionName) as this=
    static member GetConnection(?host, ?port, ?blocking) =
        let host = defaultArg host "localhost"
        let port = defaultArg port 8888
        let blocking =
            match blocking with
            | Some true -> "TRUE"
            | _ -> "FALSE"
        let connectionName = getNextSymbolName()
        loadPackage("svSocket")
        eval(sprintf "%s <- socketConnection(host='%s', port='%d', blocking='%s')" connectionName host port blocking) |> ignore
        new RemoteSession(connectionName)

    member this.connectionName = connectionName
    member this.isClosed = false

    member this.makeSafeExpr (expr: string) =
        expr.Replace("\"","\\\\\"").Replace("'", "\\\\'")
        
    member this.evalToSymbolicExpression expr =
        let expr = this.makeSafeExpr expr
        eval(sprintf """evalServer(%s, '%s')""" connectionName expr)

    member this.getHandleValue (handle: RemoteSymbolicExpression) =
        this.evalToSymbolicExpression(handle.name)

    member this.evalToHandle expr =
        let handleName = getNextSymbolName()
        let expr = this.makeSafeExpr expr
        eval(sprintf "evalServer(%s, '%s <- %s; TRUE')" connectionName handleName expr) |> ignore
        new RemoteSymbolicExpression(this.getHandleValue, handleName)

    member this.exec expr =
        let expr = this.makeSafeExpr expr
        eval(sprintf "evalServer(%s, 'exec(%s); TRUE');" this.connectionName expr) |> ignore

    member this.assign name value =
        let symbolName, se = toR value
        eval(sprintf "evalServer(%s, %s, %s)" this.connectionName name symbolName) |> ignore
        
    member this.getRemoteSymbol name =
        this.evalToSymbolicExpression name

    member this.call (packageName: string) (funcName: string) (serializedRVal:string) (namedArgs: obj[]) (varArgs: obj[]) : RemoteSymbolicExpression =
        call_ this.evalToHandle packageName funcName serializedRVal namedArgs varArgs

    member this.callFunc (packageName: string) (funcName: string) (argsByName: seq<KeyValuePair<string, obj>>) (varArgs: obj[]) : RemoteSymbolicExpression =
        callFunc_ this.evalToHandle packageName funcName argsByName varArgs

    member this.bindingInfo (name: string) : RValue =
        bindingInfo_ this.evalToSymbolicExpression name

    member val cache_getPackages = lazy(getPackages_ this.evalToSymbolicExpression)
    
    member this.getPackages ?useCache : string[] =
        let useCache = defaultArg useCache true
        match useCache with
        | true -> this.cache_getPackages.Force()
        | false -> getPackages_ this.evalToSymbolicExpression

    member this.getCached useCache (cache : Dictionary<_,_>) key lookup =
        match useCache with
        | true when cache.ContainsKey key -> cache.[key]
        | true ->
            let value = lookup()
            cache.Add(key, value)
            value
        | false -> lookup() 
    
    member this.cache_getPackageDescription = new Dictionary<string,string>()

    member this.getPackageDescription(packageName, ?useCache) : string =
        this.getCached
            (defaultArg useCache true)
            this.cache_getPackageDescription 
            packageName 
            (fun () -> getPackageDescription_ this.evalToSymbolicExpression packageName)
        
    member this.cache_getFunctionDescriptions = new Dictionary<string,Map<string,string>>()

    member this.getFunctionDescriptions(packageName, ?useCache) : Map<string, string> =
        this.getCached
            (defaultArg useCache true)
            this.cache_getFunctionDescriptions
            packageName
            (fun () -> getFunctionDescriptions_ this.exec this.evalToSymbolicExpression packageName)

    member this.packages = System.Collections.Generic.HashSet<string>()

    member this.loadPackage packageName =
        loadPackage_ this.evalToSymbolicExpression this.packages packageName

    member this.getBindings packageName =
        getBindings_ this.evalToSymbolicExpression packageName

    member this.serializeRValue = serializeRValue

    member this.deserializeRValue = deserializeRValue

    member this.close () =
        eval(sprintf "close(%s)" this.connectionName) |> ignore

    override this.Finalize () =
        if not this.isClosed then
            this.close()
