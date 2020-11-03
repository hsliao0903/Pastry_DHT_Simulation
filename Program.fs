

open System
open System.Security.Cryptography
open Akka.Actor
open Akka.FSharp

//defines
let system = ActorSystem.Create("AlexPastry")
let nodeNamePrefix = "Node"
let bFactor = 2
let rFactor = 2.0 ** (bFactor|>float) |> int
let LeafSetSize = rFactor
let NeighborSetSize = rFactor
let mutable numNodes = 1
let maxDistance = 1000
//end

//Global use

type Msg = 
    | JOIN

type NodeMessage =
    | PastryInit of string
    | Route of Msg * string

type PastryNodeMessage =
    | InitStart
    | End


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// random a integer between -coordinatRange ~ coordianteRange
//let rndCoordinate (rnd:Random)= 
//    rnd.Next(0,coordinateRange)*2 - coordinateRange

let selectActorByName name =
    select ("akka://" + system.Name + "/user/" + nodeNamePrefix + name) system

// generate a random 128bit 
let getID (rand:Random) = 
    let mutable res = 0I
    let mutable digit = 1I
    for i in 1 .. 128 do
        if rand.Next(2) > 0 then
            res <- res + digit
        digit <- digit*2I

    //printfn "res:%A\n" res
    res.ToByteArray()


let hash (plaintext: byte[]) = 
    use md5 = MD5.Create()
    let encrypt =  plaintext |> md5.ComputeHash |> BitConverter.ToString
    //printfn "encrypt:%s" (encrypt)
    encrypt.Replace("-", "")        


let node message = 
    printfn "received!"
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

let nodes (nodeMailbox:Actor<NodeMessage>) =
    let nodeName = nodeMailbox.Self.Path.Name
    let nodeIdx = nodeName.Substring(4) |> int
    let mutable nodeID = ""
    //printfn "[%s] %s" nodeName (nodeMailbox.Self.Path.ToString())
    let mutable leafSet = Set.empty
    let mutable neighborSet = Set.empty
    let routingTable = Array2D.create (ceil(Math.Log((numNodes|>float),(rFactor|>float)))|>int) (rFactor-1) ""
    //printfn "[%s] %d, %d, %A, %A, %A\n" nodeName xCoordinate yCoordinate leafSet neighborSet routingTable
    //printfn " tet = %d" (ceil(Math.Log((numNodes|>float),(rFactor|>float)))|>int)
    let rec loop() = actor {
        let! (msg: NodeMessage) = nodeMailbox.Receive()
        match msg with
            
            | PastryInit nodeA -> (* nodeA is assigned by pastryboss according to proximity metric *)

                (* random a 128 bit number and hash it with MD5 to get a 128 bit nodeID  *)
                let rnd = Random()
                nodeID <- hash (getID rnd)
                printfn "[%s] My nodeID is %A" nodeName nodeID

                (* Any initial procedures?*)
                //TODO:

                (* Send route message to nodeA, with "JOIN" type Msg *)
                selectActorByName nodeA <! Route (JOIN, nodeID)
                return! loop()
            | Route (msg, key) ->
                match (msg) with
                    | JOIN ->
                        ()
                    
                return! loop()
         
        return! loop()
    }
    loop()

let pastryBoss (proxMetric:int [,]) numNodes (pbossMailbox:Actor<PastryNodeMessage>) =
    let nodeName = pbossMailbox.Self.Path.Name
    //printfn "[%s] %s" nodeName (nodeMailbox.Self.Path.ToString())
    printfn "[%s]\n %A\n" nodeName proxMetric
    let mutable nodeCount = 0
    let mutable networkNodeSet = Set.empty
    
    let rec loop() = actor {
        let! (msg: PastryNodeMessage) = pbossMailbox.Receive()
        match msg with
            | InitStart ->
                (* all nodes are already added to the network *)
                if nodeCount = numNodes then
                    selectActorByName nodeName <! End
                nodeCount <- nodeCount + 1
                printfn "[%s] nodeCount:%d" nodeName nodeCount
                (* the node going to be add to pastry network *)
                let newNodeName = nodeNamePrefix + nodeCount.ToString()
                printfn "[%s] %s is ready to be add to the network\n" nodeName newNodeName
                
                
                (* find a closest node in the network according to the proximity metric *)
                //let rnd = Random()
                //for i in 1 .. 5 do
                //    networkNodeSet <- networkNodeSet.Add(rnd.Next(1,numNodes))
                let mutable nodeA = ""
                if not networkNodeSet.IsEmpty then
                    let mutable distance = maxDistance + 1
                    for node in networkNodeSet do
                        let row = (min node nodeCount)-1 //array starts from 0
                        let col = (max node nodeCount)-1 //array starts from 0
                        //printfn "node:%d, row:%d, col:%d nodeA:%s dis:%d\n" node row col nodeA distance
                        if proxMetric.[row,col] < distance then
                            nodeA <- node.ToString()
                            distance <- proxMetric.[row,col]
                    nodeA <- nodeNamePrefix + nodeA
                printfn "[%s] the first neighbor of %s is nodeA: %s\n" nodeName newNodeName nodeA
                (* Send this nodeA to the new-to-be-added node *)
                selectActorByName newNodeName <! PastryInit nodeA
                return! loop()

            | End ->
                printfn "The whole pastry network has been built, total nodes count: %d\n" nodeCount
         
        return! loop()
    }
    loop()

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

[<EntryPoint>]
let main argv =
    try
        let tmpNumNodes = argv.[0] |> int
        let numRequest = argv.[1] |> int
        //let inbox = Inbox.Create(system)
        
        
        
       
        // start to build the pastry network, ofcourse at least one
        numNodes <- tmpNumNodes
        printfn "[MAIN] Start, numNodes:%d, numReqquest:%d" numNodes numRequest
        if numNodes < 0 then
            printfn "numNodes should bigger than zero"
            Environment.Exit 1
        
        //generate a proximity metric, assuming all nodes are arranged into a 2D grid first
        (*
        let square x = x * x
        numNodes <- square ((ceil(sqrt(tmpNumNodes |> double))) |> int)
        printfn "newNumNode:%d" numNodes
        let tmpArray = Array2D.create (numNodes+1) 4 ""
        for i in 1 .. numNodes do
            let sideLen = sqrt(numNodes |> double) |> int
            let rightIdx = i + 1
            let leftIdx = i - 1
            let topIdx = i - sideLen
            let belowIdx = i + sideLen

            if topIdx > 0 then
                tmpArray.[i,0] <- (topIdx.ToString())
            if belowIdx <= numNodes then
                tmpArray.[i,1] <- (belowIdx.ToString())
            if (i % sideLen) <> 0 then
                tmpArray.[i,2] <- (rightIdx.ToString())
            if (i % sideLen) <> (1) then
                tmpArray.[i,3] <- (leftIdx.ToString())
            ()
        *)
        //let nborSet = (Array.filter ((<>) "") tmpArray) |> Set.ofArray
        //printf "%A\n" tmpArray
        //let ret = Array.exists (fun elem -> elem = "9") tmpArray.[5,*]
        //printf "ret:%b\n" ret

        //initial all nodes first and generate proximity metric
        let proxMetric = Array2D.create numNodes numNodes 0
        for i in 1 .. numNodes do
            // initial the proximity metric
            //printfn "i=%d\n" i
            for j in (i) .. (numNodes-1) do
                let rnd = Random()
                //printfn "%d,%d" (i-1) j
                proxMetric.[i-1,j] <- rnd.Next(0, maxDistance)
            //let nodename = nodeNamePrefix  + i.ToString()
            //spawn system nodename nodes |> ignore
        //printfn "%A\n" proxMetric

        let pastryBossRef = spawn system "pastryBoss" (pastryBoss proxMetric numNodes)
        pastryBossRef <! InitStart

       
        

        while true do
            ()
        //start building the first node
        




    with | :? IndexOutOfRangeException ->
            printfn "\n[Main] Incorrect Inputs or IndexOutOfRangeException!\n"

         | :?  FormatException ->
            printfn "\n[Main] FormatException!\n"


    0 // return an integer exit code
