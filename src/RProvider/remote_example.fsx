#r @"../../bin/RDotNet.dll"
#r @"../../bin/RInterop.dll"
#r @"../../bin/RProvider.dll"

(* README FIRST!!!
   You MUST start a session of RGui, R-Studio, or something FIRST. Next:
   1. Check and ensure you have the "svSocket" package installed.
      svSocket provides the necessary communications protocol to share the session
   2. In the remote R session: require(svSocket)
   3. In the remote R session: startSocketServer() 
      This opens a socket that can be connected to for processes sharing the session,
      such as an fsi script.
 
  Now, you are ready to run this script.
*)

open RDotNet
open RInterop
open RInteropInternal
open RProvider
open RProvider.``base``
open System
open System.Net

// define a type that can connect to a remote R session
// the type arguments are hostname, port, blocking.
// refer to openSocket in the svSocket R package
type RRSession = RemoteR<"localhost", 8888, false>
// create a new session to interact with the remote R session.
let rr = new RRSession()

// URL of a service that generates price data
let url = "http://ichart.finance.yahoo.com/table.csv?s="
 
/// Returns prices (as tuple) of a given stock for a
/// specified number of days (starting from the most recent)
let getStockPrices stock count =
    // Download the data and split it into lines
    let wc = new WebClient()
    let data = wc.DownloadString(url + stock)
    let dataLines = data.Split([| '\n' |], StringSplitOptions.RemoveEmptyEntries)
 
    // Parse lines of the CSV file and take specified
    // number of days using in the oldest to newest order
    seq { for line in dataLines |> Seq.skip 1 do
              let infos = line.Split(',')
              yield float infos.[4] }
    |> Seq.take count |> Array.ofSeq |> Array.rev

//retrieve stock price time series and compute returns
//the actual returns are computed in the remote R session
let msft = getStockPrices "MSFT" 255 |> rr.``base``.log |> rr.``base``.diff

//compute the autocorrelation of msft stock returns
let a = rr.stats.acf(msft)

//lets see if the msft returns are stationary/non-unit root
let adf = rr.tseries.adf_test(msft) 

//lets look at some pair plots
let tickers = [ "MSFT"; "AAPL"; "X"; "VXX"; "SPX"; "GLD" ]
let data = [ for t in tickers -> t, getStockPrices t 255 |> rr.``base``.log |> rr.``base``.diff ]
let df = rr.``base``.data_frame(namedParams data)
rr.graphics.pairs(df)
