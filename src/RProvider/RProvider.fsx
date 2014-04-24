#nowarn "211"
// Try including various folders where RProvider might be (version updated by FAKE)
#I "../../bin"
#I "../bin"
#I "bin"
#I "lib"
#I "packages/RInterop.1.0.5/lib"
#I "../packages/RInterop.1.0.5/lib"
#I "../../packages/RInterop.1.0.5/lib"
#I "../../../packages/RInterop.1.0.5/lib"
#I "packages/RProvider.1.0.5/lib"
#I "../packages/RProvider.1.0.5/lib"
#I "../../packages/RProvider.1.0.5/lib"
#I "../../../packages/RProvider.1.0.5/lib"
// Reference RProvider and RDotNet (which should be copied to the same directory)
#r "RDotNet.dll"
#r "RInterop.dll"
#r "RProvider.dll"
open RInterop
open RProvider

do fsi.AddPrinter(fun (synexpr:RDotNet.SymbolicExpression) -> RSafe <| fun () -> synexpr.Print())
