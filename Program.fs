

open System
open System.Security.Cryptography
open System.Globalization
open System.Text
open Akka.Actor
open Akka.FSharp

//defines
let system = ActorSystem.Create("AlexPastry")
let nodeNamePrefix = "Node"
let nodePastryBoss = "pastryBoss"
let bFactor = 2
let rFactor = 2.0 ** (bFactor|>float) |> int
let LeafSetSize = rFactor  //may be set to 2*rFactor
let NeighborSetSize = rFactor //may be set to 2*rFactor
let mutable numNodes = 1
let maxDistance = 1000
//end

//Global use

type Msg = 
    | JOIN

type NodeMessage =
    | PastryInit of int
    | PastryInitDone of int
    | Route of Msg * string * int * int
    | InitNborSet of Map<int, string>
    | InitLeafSet of Map<int, string>
    | InitRoutingTable of int
    | UpdateStates of int * string

type PastryNodeMessage =
    | AddNewNode
    | AddComplete of string
    | End


type ProxityMetric (numNodes, maxDistance) =
   let mutable pm = Map.empty
   let rand = Random()
   do for i in 1 .. numNodes do
       for j in i .. (numNodes-1) do
           let d = rand.Next(0,maxDistance)
           pm <- pm.Add((i,j),d)
           pm <- pm.Add((j,i),d)
 
   member val FullMetric = pm
   member this.GetDistance i j = pm.[i,j]
 
let pm = ProxityMetric (numNodes,maxDistance)


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// random a integer between -coordinatRange ~ coordianteRange
//let rndCoordinate (rnd:Random)= 
//    rnd.Next(0,coordinateRange)*2 - coordinateRange

let getNodeName idx =
    nodeNamePrefix + idx.ToString()

let selectActorByName name =
    select ("akka://" + system.Name + "/user/" + name) system

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

let hexToBigint hexString =
    bigint.Parse(hexString, NumberStyles.HexNumber)

(* Since hexToBigint would return a 2's coplment value, which might have a negative value *)
let diffNumeric hexStr1 hexStr2 =
    let mutable res = (hexToBigint hexStr1) - (hexToBigint hexStr2)
    if res < 0I then
        0I - res
    else
        res


let getSmallestDiffLeafNode leafSet key curKey curID=
    let mutable outofRange = false
    let mutable inUpperBound = false
    let mutable inLowerBound = false
    let mutable closestNodeIdx = 0
    let mutable tmpDiff = 0I //bigint type
    
    for KeyValue(leafID, leafKey) in leafSet do
        (* Count the distace of |D - Li|, and keep the smallest one in closestNodeIdx *)
        let difference = diffNumeric leafKey key
        if closestNodeIdx = 0 then
            closestNodeIdx <- leafID
            tmpDiff <- difference
        else
            if difference < tmpDiff then
                closestNodeIdx <- leafID
                tmpDiff <- difference
        (* Check if the key is in the range of Leaf Set *)
        if (hexToBigint key) <= (hexToBigint leafKey) then
            inUpperBound <- true
        if (hexToBigint key) >= (hexToBigint leafKey) then
            inLowerBound <- true
    (* If we have one leaf key is bigger and one leaf is smaller than the target D, than it must be in the range of leaf set *)
    outofRange <- inUpperBound && inLowerBound
    
    (* If it is out of range, than we return 0, means that we can't not find any Li*)
    (* If D is in the range of leaf set,
    also check the difference of present nodeID and key,
    return the closest node index with the smallest difference to D *)
    if outofRange then
        0
    else
        if diffNumeric curKey key < tmpDiff then
            closestNodeIdx <- curID
        closestNodeIdx

(* return the common prefix length for these two input strings *)
let shl (keyA:string) (keyB:string) =
    let len = keyA.Length
    if len <> keyB.Length then
        printfn "Somthing is wrong in shl function\n\n"
        Environment.Exit 1
    
    let mutable idx = -1
    for i in 0 .. len-1 do
        if keyA.[i] <> keyB.[i] then
            if idx = -1 then
                idx <- i
    if idx = -1 then
        idx <- len
    idx
    



        
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

let nodes (proxMetric:int [,]) (nodeMailbox:Actor<NodeMessage>) =
    let nodeName = nodeMailbox.Self.Path.Name
    let nodeIdx = nodeName.Substring(4) |> int
    let mutable nodeID = ""
    //printfn "[%s] %s" nodeName (nodeMailbox.Self.Path.ToString())
    let mutable leafSet = Map.empty
    let mutable neighborSet = Map.empty
    let routingTable = Array2D.create (ceil(Math.Log((numNodes|>float),(rFactor|>float)))|>int) (rFactor-1) ""
    //printfn "[%s] %d, %d, %A, %A, %A\n" nodeName xCoordinate yCoordinate leafSet neighborSet routingTable
    //printfn " tet = %d" (ceil(Math.Log((numNodes|>float),(rFactor|>float)))|>int)
    let rec loop() = actor {
        let! (msg: NodeMessage) = nodeMailbox.Receive()
        match msg with
            
            | PastryInit nodeAIdx -> (* nodeA is assigned by pastryboss according to proximity metric *)

                (* random a 128 bit number and hash it with MD5 to get a 128 bit nodeID for myself *)
                let rnd = Random()
                nodeID <- hash (getID rnd)
                printfn "[%s] My nodeID is %A" nodeName nodeID

                (* Any other initial procedures? *)
                //TODO:

                (* If nodeA is NULL, means it is the very first node in the pastry network, inform the pastry boss for completion *)
                if nodeAIdx = 0 then
                    selectActorByName nodePastryBoss <! AddComplete nodeName
                    return! loop()
                
                (* Request nodeA's neighbor set for initailzing my own neighbor set*)
                

                (* Send route message to nodeA, with "JOIN" type Msg *)
                let hopCount = 0
                selectActorByName (getNodeName nodeAIdx) <! Route (JOIN, nodeID, nodeIdx, hopCount)
                // TODO: request nodeA's leaf set for initialize my own leaf set
                
                return! loop()

            | PastryInitDone nodeZIdx ->
                printfn "[%s] My nodeZ is Node%d" nodeName nodeZIdx
                
                (* Inform all nodes in my leaf set *)
                for KeyValue(nodeIdx, nodeKey) in leafSet do
                    selectActorByName (getNodeName nodeIdx) <! UpdateStates (nodeIdx, nodeID)
                (* Inform all nodes in my routing table *)
                //TODO: 
                (* Inform all nodes in my neighbor set *)
                for KeyValue(nodeIdx, nodeKey) in neighborSet do
                    selectActorByName (getNodeName nodeIdx) <! UpdateStates (nodeIdx, nodeID)

                (* Notice the pastry boss that I am successfully added to the network *)
                //TODO: should wait for all the updates are complete?
                selectActorByName nodePastryBoss <! AddComplete nodeName
                return! loop()

            (* make sure the message sender would always be the origin "new Node", use message "forward" *)
            | Route (msg, key, senderIdx, hopCount) ->
                match (msg) with
                    | JOIN ->
                        let senderNodeName = nodeMailbox.Sender().Path.Name
                        let senderNodeIdx = senderNodeName.Substring(4) |> int
                        printfn "[%s] Receive JOIN route from \"%s\"" nodeName senderNodeName
                        //let mutable nextNodeIdx = 0 // initial the nextNode
                        (* forward the msg according to routing algorithm *)
                        //First, search in the leaf set
                        (*
                        if leafSet.Count < LeafSetSize then     // Question: Is it correct here?
                            // add the msg sender to my leafSet
                            //TODO: send my leaf set to the sender
                            leafSet <- leafSet.Add(senderNodeIdx, key)
                            //printfn "%A\n" (bigint.Parse("2", NumberStyles.HexNumber))
                            printfn "[%s] Add %s:%A into my leafSet\n" nodeName senderNodeName key
                        else
                        *)

                        //check if it is in the range of leafset
                        let mutable nextNodeIdx = getSmallestDiffLeafNode leafSet key nodeID nodeIdx
                        if nextNodeIdx <> 0 then
                            // forward JOIN msg to this leaf node
                            
                            //printfn "[%s] NodeZ found which is Node%d" nodeName nodeZIdx
                            if nextNodeIdx <> nodeIdx then
                                //nextNodeName <- nodeNamePrefix + nodeZIdx.ToString()
                                //TODO: send my (hopcount) th row of routing table to sender
                                selectActorByName (getNodeName senderIdx) <! InitRoutingTable hopCount
                                selectActorByName (getNodeName nextNodeIdx) <! Route (msg, key, senderIdx, hopCount+1)
                                return! loop()
                            else
                                // I am the nodeZ then, send my leafnode set to the joining node, tell it that nodeZ is found
                                selectActorByName (getNodeName senderIdx) <! InitLeafSet leafSet
                                selectActorByName (getNodeName senderIdx) <! PastryInitDone nextNodeIdx
                                return! loop()
                        else
                            // the common prefix length between key and present nodeID
                            let prefixLen = shl key nodeID
                            if false then
                                //TODO: use routing table here, to find a nodeIdx to send the join route meassage
                                
                                ()
                            else
                                // got through all the nodes in state tables 
                                //such that shl(nodeID, key) >= prefixLen and |nodeID - key| < |present nodeID - key|
                                
                                //let mutable tmpNodeIdx = 0
                                (* Search in leaf set *)
                                for KeyValue(nodeIdx, nodeKey) in leafSet do
                                    if (shl nodeKey key) >= prefixLen && (diffNumeric nodeKey key) < (diffNumeric nodeID key) then
                                        nextNodeIdx <- nodeIdx
                                    if nextNodeIdx <> 0 then
                                        printfn "[%s] Rare case, found next node in leaf set" nodeName
                                        selectActorByName (getNodeName senderIdx) <! InitRoutingTable hopCount
                                        selectActorByName (getNodeName nextNodeIdx) <! Route (msg, key, senderIdx, hopCount+1)
                                        return! loop()
                                (* Search in routing table *)
                                //TODO:
                                (* Search in neighbor set*)
                                for KeyValue(nodeIdx, nodeKey) in neighborSet do
                                    if (shl nodeKey key) >= prefixLen && (diffNumeric nodeKey key) < (diffNumeric nodeID key) then
                                        nextNodeIdx <- nodeIdx
                                    if nextNodeIdx <> 0 then
                                        printfn "[%s] Rare case, found next node in neighbor set" nodeName
                                        selectActorByName (getNodeName senderIdx) <! InitRoutingTable hopCount
                                        selectActorByName (getNodeName nextNodeIdx) <! Route (msg, key, senderIdx, hopCount+1)
                                        return! loop()
                                
                                (* If there is no any node found, then I will be the nodeZ, because there are too less nodes in the network*)
                                selectActorByName (getNodeName senderIdx) <! InitLeafSet leafSet
                                selectActorByName (getNodeName senderIdx) <! PastryInitDone nextNodeIdx
                                return! loop()
                                
                                    
                        //TODO: routing algorithm
                        (* Send my own state table to the sender *)
                        //TODO: send back my routing table
                        (* If We found the nodeZ, which is the numerically closest node to the new node in whole network *)
                        //TODO: update neighbor table, send back my state tables?
                        // infrom the new node that i am the destination node
                        //nodeMailbox.Sender() <! PastryInitDone nextNodeName
                        
                printfn "[%s] [ROUTE] [JOIN] Error!" nodeName
                Environment.Exit 1  
                return! loop()
            | InitLeafSet newLeafSet ->
                //initial my leafSet, is this right? Copy the newMap to local map?
                leafSet <- newLeafSet
                return! loop()
            | InitNborSet newNborSet ->
                neighborSet <- newNborSet
                return! loop()
            | InitRoutingTable row->
                //TODO: update my routing table row i according this row number
                return! loop()
            | UpdateStates (newNodeIdx, newNodeID) ->
                //TODO: update my own state tables according this new joined node

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
            | AddNewNode ->
                (* all nodes are already added to the network *)
                if nodeCount = numNodes then
                    selectActorByName nodeName <! End
                    return! loop()

                (* spawn the node going to be add to pastry network*)
                nodeCount <- nodeCount + 1
                //printfn "[%s] nodeCount:%d" nodeName nodeCount
                let newNodeName = nodeNamePrefix + nodeCount.ToString()
                spawn system newNodeName (nodes proxMetric) |> ignore
                printfn "[%s] Start to add \"%s\" to the pastry network\n" nodeName newNodeName
                
                
                (* find a closest node in the network according to the proximity metric *)
                //let rnd = Random()
                //for i in 1 .. 5 do
                //    networkNodeSet <- networkNodeSet.Add(rnd.Next(1,numNodes))
                let mutable nodeAIdx = 0
                if not networkNodeSet.IsEmpty then
                    let mutable distance = maxDistance + 1
                    for idx in networkNodeSet do
                        let row = (min idx nodeCount)-1 //array starts from 0
                        let col = (max idx nodeCount)-1 //array starts from 0
                        //printfn "node:%d, row:%d, col:%d nodeA:%s dis:%d\n" node row col nodeA distance
                        if proxMetric.[row,col] < distance then
                            nodeAIdx <- idx
                            distance <- proxMetric.[row,col]
                    //nodeA <- nodeNamePrefix + nodeA
                printfn "[%s] the first neighbor of %s is nodeA: Node%d\n" nodeName newNodeName nodeAIdx

                (* Send this nodeA to the new-to-be-added node *)
                selectActorByName newNodeName <! PastryInit nodeAIdx

                return! loop()
            | AddComplete nodeName ->
                (*Add the node index to the network list because it is already added to the network*)
                let nodeIdx = nodeName.Substring(4) |> int
                networkNodeSet <- networkNodeSet.Add(nodeIdx)
                printfn "[%s] Node%d is successfully added to the Pastry network\n" nodeName nodeIdx

                (* Then send a AddNewNode msg to myself to add another node to the network *)
                pbossMailbox.Self <! AddNewNode
                return! loop()
            
            | End ->
                (* All the number of nodes are added to the pastry network *)
                printfn "The whole pastry network has been built, total node counts: %d\n" nodeCount
                printfn "[%s] networkNodeSet:\n %A\n" nodeName networkNodeSet

                //TODO: may start to handle request
                Environment.Exit 1
         
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

        let mutable testSet = Map.empty

        let mutable tmpSet = Map.empty
        tmpSet <- tmpSet.Add(1, "abc")
        tmpSet <- tmpSet.Add(23, "124j1oij124i1")
        printfn "tmpset\n%A" tmpSet
        printfn "testSEt\n%A" testSet
        
        printfn "\ntesttstesetsetetsetsetsetest\n"
        testSet <- tmpSet
        printfn "tmpset\n%A" tmpSet
        printfn "testSEt\n%A" testSet
        //Environment.Exit 1
        // start to build the pastry network, ofcourse at least one
        (* get the input arguments *)
        numNodes <- tmpNumNodes
        printfn "[MAIN] Start, numNodes:%d, numReqquest:%d" numNodes numRequest
        if numNodes < 0 then
            printfn "numNodes should bigger than zero"
            Environment.Exit 1

        (* generate proximity metric *)
        let proxMetric = Array2D.create numNodes numNodes 0
        for i in 1 .. numNodes do
            //printfn "i=%d\n" i
            for j in (i) .. (numNodes-1) do
                let rnd = Random()
                //printfn "%d,%d" (i-1) j
                proxMetric.[i-1,j] <- rnd.Next(0, maxDistance)
        //printfn "%A\n" proxMetric

        (* Let pastry boss to help building the pastry network *)
        let pastryBossRef = spawn system nodePastryBoss (pastryBoss proxMetric numNodes)
        pastryBossRef <! AddNewNode
        

        
        (* Future use *)
        // TODO: Application use for request?
        while true do
            ()

    with | :? IndexOutOfRangeException ->
            printfn "\n[Main] Incorrect Inputs or IndexOutOfRangeException!\n"

         | :?  FormatException ->
            printfn "\n[Main] FormatException!\n"


    0 // return an integer exit code
