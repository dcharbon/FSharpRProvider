namespace RProvider

open Microsoft.FSharp.Core.CompilerServices
open RDotNet
open RInterop
open RInterop.Logging
open RInterop.Remote
open RInterop.RInteropInternal
open Samples.FSharp.ProvidedTypes
open System
open System.Collections.Generic
open System.Reflection

module RProviderRemoteRuntime =
    let mutable remoteSessions = Map.empty

    let getRemoteSession host port blocking =
        match remoteSessions.ContainsKey (host,port,blocking) with
        | true -> 
            logf "Returning cached session for (%s,%d,%b)" host port blocking
            remoteSessions.[(host,port,blocking)]
        | false ->
            logf "Creating a new remote session for (%s,%d,%b)" host port blocking
            let remoteSession = RemoteSession.GetConnection(host, port, blocking)
            remoteSessions <- remoteSessions.Add((host,port,blocking), remoteSession)
            remoteSession

    let generateTypes (session: RemoteSession) (parentType: ProvidedTypeDefinition) =
        RSafe <| fun () ->
        // Expose all packages available in the remote R session as members
        logf "RProviderRemote.generateTypes: getting packages"
        for package in session.getPackages()  do
            let pty = ProvidedTypeDefinition(package, Some(typeof<obj>), HideObjectMethods = true)
            pty.AddXmlDocDelayed <| fun () -> session.getPackageDescription package
            
            pty.AddMembersDelayed( fun () ->
              [ session.loadPackage package
                let bindings = session.getBindings package
                let titles = lazy session.getFunctionDescriptions package
                for name, rval in Map.toSeq bindings do
                    let memberName = makeSafeName name
                    let serializedRVal = session.serializeRValue rval
                    match rval with
                    | RInteropInternal.RValue.Function(paramList, hasVarArgs) ->
                      let paramList = [ for p in paramList -> 
                                                ProvidedParameter(makeSafeName p,  typeof<obj>, optionalValue=null)

                                        if hasVarArgs then
                                            yield ProvidedParameter("paramArray", typeof<obj[]>, optionalValue=null, isParamArray=true)
                                      ]
                      let paramCount = paramList.Length
                        
                      let pm = ProvidedMethod(
                                    methodName = memberName,
                                    parameters = paramList,
                                    returnType = typeof<SymbolicExpression>,
                                    InvokeCode = fun args -> if args.Length <> paramCount+1 then // expect arg 0 is connection
                                                                failwithf "Expected %d arguments and received %d" paramCount args.Length
                                                             if hasVarArgs then
                                                                let namedArgs = 
                                                                    Array.sub (Array.ofList args) 1 (paramCount-1)
                                                                    |> List.ofArray
                                                                let namedArgs = Quotations.Expr.NewArray(typeof<obj>, namedArgs)
                                                                let varArgs = args.[paramCount]
                                                                <@@ ((%%args.[0]:obj) :?> RemoteSession).call package name serializedRVal %%namedArgs %%varArgs @@>
                                                             else
                                                                let namedArgs = Quotations.Expr.NewArray(typeof<obj>, args)                                            
                                                                <@@ ((%%args.[0]:obj) :?> RemoteSession).call package name serializedRVal %%namedArgs [||] @@> )
                      pm.AddXmlDocDelayed (fun () -> match titles.Value.TryFind name with 
                                                     | Some docs -> docs 
                                                     | None -> "No documentation available")
                      yield pm :> MemberInfo

                      let pdm = ProvidedMethod(
                                    methodName = memberName,
                                    parameters = [ ProvidedParameter("paramsByName",  typeof<IDictionary<string,obj>>) ],
                                    returnType = typeof<SymbolicExpression>,
                                    InvokeCode = fun args -> if args.Length <> 1 then
                                                               failwithf "Expected 1 argument and received %d" args.Length
                                                             let argsByName = args.[0]
                                                             <@@  let vals = %%argsByName: IDictionary<string,obj>
                                                                  let valSeq = vals :> seq<KeyValuePair<string, obj>>
                                                                  ((%%args.[0]:obj) :?> RemoteSession).callFunc package name valSeq null @@> )
                      yield pdm :> MemberInfo

                    | RValue.Value ->
                       yield ProvidedProperty(
                               propertyName = memberName,
                               propertyType = typeof<SymbolicExpression>,
                               GetterCode = fun args -> <@@ ((%%args.[0]:obj) :?> RemoteSession).call package name serializedRVal [| |] [| |] @@>) :> MemberInfo
              ]
            )

            parentType.AddMember pty
            let ptyName = pty.Name
            let prop =
                ProvidedProperty(
                    propertyName = pty.Name,
                    propertyType = pty,
                    GetterCode = fun args -> <@@ %%args.[0] @@>
                    )
            parentType.AddMember(prop)


open RProviderRemoteRuntime

[<TypeProvider>]
type public RProviderRemote(cfg:TypeProviderConfig) as this =
    inherit TypeProviderForNamespaces()

    let asm = System.Reflection.Assembly.GetExecutingAssembly()
    let ns = "RProvider"
    let baseType = typeof<obj>
    let staticParams = 
        [ ProvidedStaticParameter("host", typeof<string>)
          ProvidedStaticParameter("port", typeof<int>)
          ProvidedStaticParameter("blocking", typeof<bool>) ]

    let rremoteType = ProvidedTypeDefinition(asm, ns, "RemoteR", Some baseType)
            
    do rremoteType.DefineStaticParameters(
        parameters=staticParams,
        instantiationFunction=(fun typeName parameterValues ->
            RSafe <| fun () ->
            let host = parameterValues.[0] :?> string
            let port = parameterValues.[1] :?> int
            let blocking = parameterValues.[2] :?> bool

            // create an instance of the remote session for type discovery
            let remoteSession = getRemoteSession host port blocking
        
            // the type given the static parameters
            let sessionType = 
                ProvidedTypeDefinition(
                    asm,
                    ns,
                    typeName,
                    baseType = Some baseType)

            sessionType.AddXmlDoc <| sprintf
                "A strongly typed interface to the R session hosted at %s, port %d through svSocket"
                host
                port
            
            generateTypes remoteSession sessionType
            
            let ctor =
                ProvidedConstructor(
                    parameters = [],
                    InvokeCode = fun args -> <@@ getRemoteSession host port blocking :> obj @@>)
            ctor.AddXmlDoc "Initialize a connected R session hosted through svSocket."
            sessionType.AddMember ctor

            let sessionEvalToHandle = 
                ProvidedMethod(
                    methodName = "evalToHandle",
                    parameters = [ ProvidedParameter("expr",  typeof<string>) ],
                    returnType = typeof<RemoteSymbolicExpression>,
                    InvokeCode = fun args -> if args.Length <> 2 then
                                               failwithf "Expected 2 argument and received %d" args.Length
                                             <@@ ((%%args.[0]:obj) :?> RemoteSession).evalToHandle %%args.[1] @@>
                    )
            sessionType.AddMember sessionEvalToHandle

            let sessionEvalToSymbolicExpression = 
                ProvidedMethod(
                    methodName = "eval",
                    parameters = [ ProvidedParameter("expr",  typeof<string>) ],
                    returnType = typeof<SymbolicExpression>,
                    InvokeCode = fun args -> if args.Length <> 2 then
                                               failwithf "Expected 2 argument and received %d" args.Length
                                             <@@ ((%%args.[0]:obj) :?> RemoteSession).evalToSymbolicExpression %%args.[1] @@>
                    )
            sessionType.AddMember sessionEvalToSymbolicExpression

            let sessionAssign = 
                ProvidedMethod(
                    methodName = "assign",
                    parameters = [ ProvidedParameter("name",  typeof<string>); ProvidedParameter("value", typeof<obj>) ],
                    returnType = typeof<unit>,
                    InvokeCode = fun args -> if args.Length <> 3 then
                                               failwithf "Expected 3 argument and received %d" args.Length
                                             <@@ ((%%args.[0]:obj) :?> RemoteSession).assign %%args.[1] %%args.[2] @@>
                    )
            sessionType.AddMember sessionAssign

            let sessionGet = 
                ProvidedMethod(
                    methodName = "get",
                    parameters = [ ProvidedParameter("name",  typeof<string>) ],
                    returnType = typeof<SymbolicExpression>,
                    InvokeCode = fun args -> if args.Length <> 2 then
                                               failwithf "Expected 2 argument and received %d" args.Length
                                             <@@ ((%%args.[0]:obj) :?> RemoteSession).getRemoteSymbol %%args.[1] @@>
                    )
            sessionType.AddMember sessionGet

            let sessionFinalize =
                ProvidedMethod(
                    methodName = "Finalize",
                    parameters = [],
                    returnType = typeof<unit>,
                    InvokeCode = fun args -> <@@ ((%%args.[0]:obj) :?> RemoteSession).close() @@>
                    )
            sessionType.DefineMethodOverride(sessionFinalize, sessionType.GetMethod "Finalize")

            let sessionRemoteSessionProperty = 
                ProvidedProperty(
                    propertyName = "_interopSession",
                    propertyType = typeof<RemoteSession>,
                    GetterCode = fun args -> <@@ (%%args.[0]:obj) :?> RemoteSession @@>
                    )
            sessionType.AddMember sessionRemoteSessionProperty

            sessionType
        ))

    do this.AddNamespace(ns, [rremoteType])

[<TypeProviderAssembly>]
do()
