open System
open System.Security.Cryptography
open System.Globalization
open System.Text
open Akka.Actor
open Akka.FSharp
open Pastry.Metric


//defines
let system = ActorSystem.Create("AlexPastry")
let nodeNamePrefix = "Node"
let nodePastryBoss = "pastryBoss"
let bFactor = 1
let rFactor = 2.0 ** (bFactor|>float) |> int
let LeafSetSize = rFactor  
let NeighborSetSize = rFactor //may be set to 2*rFactor
//let mutable numNodes = 1
let maxDistance = 1000
let keyBits = 128
//end
let mutable nrow = floor (keyBits/bFactor|>float) |> int
let ncol = rFactor // not rFactor-1
let hashedIdLength = rFactor
let mutable proxMetric = ProxityMetric(0, 0)
//Global use
//let mutable proxMetric = ProxityMetric(numNodes, maxDistance)



//let mutable test = ProxityMetric(0,0)

type RTEntry(ownerID:string, ownerIdx:int, nodeID:string, nodeIdx:int) as self =
    let mutable owner = ownerID
    let mutable node = nodeID
    let mutable oIdx = ownerIdx
    let mutable nIdx = nodeIdx
    let mutable commonPrefix = ""
    let mutable nextDigit = ' '
    let mutable restOfID = ""
    let mutable dist = proxMetric.GetDistance oIdx nIdx

    let len = ownerID.Length
    do 
        if nodeID <> "" then self.OwnerUpdate ownerID oIdx
    
    member this.OwnerID with get() = owner 
    member this.NodeID with get() = node
    member this.OwnerIdx with get() = oIdx
    member this.NodeIdx with get() = nIdx
    member this.CommonPrefix with get() = commonPrefix
    member this.NextDigit with get() = nextDigit
    member this.RestOfID with get() = restOfID
    member this.Distance with get() = dist
    
    // Used when this entry was send to a new owner(node) for init
    member this.OwnerUpdate (newOwnerID:string) (newOwnerIdx: int) =
        owner <- newOwnerID
        oIdx <- newOwnerIdx
        dist <- proxMetric.GetDistance oIdx nIdx
        let mutable i = 0
        while (i < node.Length) && (owner.[i] = node.[i]) do
            i <- i + 1
        if i > 0 then commonPrefix <- node.[0..i-1]
        if i < len then nextDigit <- node.[i]
        if i + 1 < len then restOfID <- node.[i+1 .. len]
    
    // Used when the owner update with the received "join" message from a new node
    member this.NodeUpdate (newNodeID:string) (newNodeIdx:int)=
        node <- newNodeID
        nIdx <- newNodeIdx
        dist <- proxMetric.GetDistance oIdx nIdx
        let mutable i = 0
        while (i < min owner.Length node.Length) && (owner.[i] = node.[i]) do
            i <- i + 1
        if i > 0 then commonPrefix <- node.[0..i-1]
        if i < len then nextDigit <- node.[i]
        if i + 1 < len then restOfID <- node.[i+1 .. len]


type RoutingTable (ownerID:string, ownerIdx: int) as self =
    
    let mutable tb = Array2D.create nrow ncol (RTEntry(ownerID, ownerIdx, "", -1))
    
    let closer idx1 idx2 = 
        let dist1 = (proxMetric.GetDistance self.OwnerIdx idx1)
        let dist2 = (proxMetric.GetDistance self.OwnerIdx idx2)
        if dist1 < dist2 then idx1 else idx2

    member this.Table with get() = tb 
    member this.OwnerID = ownerID
    member this.OwnerIdx = ownerIdx

    member this.GetEntry(i:int,j:int) =
        this.Table.[i,j]

    (* Modify the owner of each entry from the row of existing Node *)
    member this.InitWith (nHob:int) (newRTB:RoutingTable) =
        tb.[nHob,*] <- 
            newRTB.Table.[nHob,*] 
            |> Array.map(fun entry -> 
                entry.OwnerUpdate this.OwnerID this.OwnerIdx
                entry
                )
    
    (* Used when the owner receive the join message from a new node *)
    member this.Update (newNodeID:string, newNodeIdx:int) =
        //find common prefix
        // i indicates the length of the common prefix
        let mutable i = 0
        while this.OwnerID.[i] = newNodeID.[i] do
            i <- i + 1
        let nextDigit = newNodeID.[i]
        let j =
            if nextDigit >= '0' && nextDigit <= '9' 
            then int nextDigit - int '0'
            else (int nextDigit - int 'A') + 10 //nextDigit in 'A' to 'F'
        //printfn "i:%d, j:%d" i j

        (* If the new node is closer to the owner than the present node, replace it*)
        if i < nrow && j < ncol && (newNodeIdx = closer newNodeIdx (tb.[i,j].NodeIdx)) then
            tb.[i,j].NodeUpdate newNodeID newNodeIdx

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type Msg = 
    | JOIN

type NodeMessage =
    | PastryInit of int
    | PastryInitDone of int
    | Route of Msg * string * int * int
    | InitNborSet of Map<int, string>
    | InitLeafSet of Map<int, string>
    | InitRoutingTable of int * RoutingTable
    | UpdateStates of int * string

type PastryNodeMessage =
    | AddNewNode
    | AddComplete of string
    | End





////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

let hexCharToInt (hexChar:char) =
    if hexChar >= '0' && hexChar <= '9' then 
        int hexChar - int '0'
    else 
        (int hexChar - int 'A') + 10


let getNodeName idx =
    nodeNamePrefix + idx.ToString()

let selectActorByName name =
    select ("akka://" + system.Name + "/user/" + name) system

// generate a random 128bit 
let getID (rand:Random) = 
    let mutable res = 0I
    let mutable digit = 1I
    for i in 1 .. keyBits do
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

(* hex1 minus hex2 *)
let diff hexStr1 hexStr2 =
    (hexToBigint hexStr1) - (hexToBigint hexStr2)


let getSmallestDiffLeafNode leafSet key curKey curID=
    let mutable outofRange = false
    let mutable inUpperBound = false
    let mutable inLowerBound = false
    let mutable closestNodeIdx = 0
    let mutable tmpDiff = 0I //bigint type
    
    for KeyValue(leafIdx, leafKey) in leafSet do
        (* Count the distace of |D - Li|, and keep the smallest one in closestNodeIdx *)
        let difference = diffNumeric leafKey key
        if closestNodeIdx = 0 then
            closestNodeIdx <- leafIdx
            tmpDiff <- difference
        else
            if difference < tmpDiff then
                closestNodeIdx <- leafIdx
                tmpDiff <- difference
        (* Check if the key is in the range of Leaf Set *)
        if (hexToBigint key) <= (hexToBigint leafKey) then
            inUpperBound <- true
        if (hexToBigint key) >= (hexToBigint leafKey) then
            inLowerBound <- true
    (* If we have one leaf key is bigger and one leaf is smaller than the target D, than it must be in the range of leaf set *)
    outofRange <- not (inUpperBound && inLowerBound)
    
    (* If it is out of range, than we return 0, means that we can't not find any Li*)
    (* If D is in the range of leaf set,
    also check the difference of present nodeID and key,
    return the closest node index with the smallest difference to D *)
    if outofRange then
        0
    else
        if (diffNumeric curKey key) < tmpDiff then
            closestNodeIdx <- curID
        closestNodeIdx

(* return the common prefix length for these two input strings *)
let shl (keyA:string) (keyB:string) =
    let len = keyA.Length
    if len <> keyB.Length then
        printfn "Somthing is wrong in shl function, keyA:%s, keyB:%s\n\n" keyA keyB
        Environment.Exit 1
    
    let mutable idx = -1
    for i in 0 .. len-1 do
        if keyA.[i] <> keyB.[i] then
            if idx = -1 then
                idx <- i
    if idx = -1 then
        idx <- len
    idx
    

let updateLeafSet curNodeID (leafSet:Map<int, string>) newNodeIdx newNodeID = 
    let halfLeafSetSize = LeafSetSize/2
    let mutable resSet = Map.empty
    let mutable smallerCount = 0
    let mutable biggerCount = 0
    let mutable smallestIdx = 0
    let mutable tmpSmallestID = ""
    let mutable biggestIdx = 0
    let mutable tmpBiggestID = ""

    //printfn "[updateLeafSet]\n%A" leafSet
    for KeyValue(nodeIdx, nodeID) in leafSet do
        if (diff nodeID curNodeID) < 0I then
            smallerCount <- smallerCount + 1
            if smallestIdx = 0 then
                smallestIdx <- nodeIdx
                tmpSmallestID <- nodeID
            else 
                if (diff nodeID tmpSmallestID) < 0I then
                    smallestIdx <- nodeIdx
                    tmpSmallestID <- nodeID   
        else if (diff nodeID curNodeID) > 0I then
            biggerCount <- biggerCount + 1
            if biggestIdx = 0 then
                biggestIdx <- nodeIdx
                tmpBiggestID <- nodeID
            else 
                if (diff nodeID tmpBiggestID) > 0I then
                    biggestIdx <- nodeIdx
                    tmpBiggestID <- nodeID
        else
            printfn "[asdfe] curNodeID:%s = leafnodeID:%s" curNodeID nodeID
            Environment.Exit 1

    //printfn "[updateLeafSet]\nscount:%d bcount:%d, smallest:%d:%s, biggest:%d:%s" smallerCount biggerCount smallestIdx tmpSmallestID biggestIdx tmpBiggestID
    
    if (diff newNodeID curNodeID) < 0I then
        // the new nodeID is smaller than the present nodeID
        if smallerCount < halfLeafSetSize then
            resSet <- leafSet.Add(newNodeIdx, newNodeID)
        else
            if (diff newNodeID tmpSmallestID) > 0I then
                resSet <- leafSet.Remove(smallestIdx)
                resSet <- resSet.Add(newNodeIdx, newNodeID)
            else
                resSet <- leafSet
    else if (diff newNodeID curNodeID) > 0I then
        if biggerCount < halfLeafSetSize then
            resSet <- leafSet.Add(newNodeIdx, newNodeID)
        else
            if (diff newNodeID tmpBiggestID) < 0I then
                resSet <- leafSet.Remove(biggestIdx)
                resSet <- resSet.Add(newNodeIdx, newNodeID)
            else
                resSet <- leafSet
    else
        printfn "[egtuy] curNodeID:%s = newNodeID:%s" curNodeID newNodeID
        Environment.Exit 1
    resSet

let getProx (proxMetric:int [,]) i j =
    let row = (min i j)-1 //array starts from 0
    let col = (max i j)-1 //array starts from 0
    proxMetric.[row,col]

let updateNeighborSet curNodeIdx (nborSet:Map<int, string>) newNodeIdx newNodeID =
    let mutable resSet = Map.empty
    let mutable farrestIdx = 0
    let mutable tmpFarrestProx = -1
    let mutable nborCount = 0
    //printfn "[updateNeighborSet]\n%A" nborSet
    for KeyValue(nodeIdx, nodeID) in nborSet do
        nborCount <- nborCount + 1
        //let prox = getProx proxMetric curNodeIdx nodeIdx
        let prox = proxMetric.GetDistance curNodeIdx nodeIdx
        if prox > tmpFarrestProx then
            farrestIdx <- nodeIdx
            tmpFarrestProx <- prox
        if curNodeIdx = nodeIdx then
            printfn "[ejoem] curNodeIdx = nodeIdx"
            Environment.Exit 1
    //printfn "[updateNeighborSet]\n farrestIdx:%d tmpFarrestProx:%d" farrestIdx tmpFarrestProx
    
    if nborCount < NeighborSetSize then
        resSet <- nborSet.Add(newNodeIdx, newNodeID)
    else
        //let prox = getProx proxMetric curNodeIdx newNodeIdx
        let prox = proxMetric.GetDistance curNodeIdx newNodeIdx
        if prox < tmpFarrestProx then
            resSet <- nborSet.Remove(farrestIdx)
            resSet <- resSet.Add(newNodeIdx, newNodeID)
        else
            resSet <- nborSet
    resSet

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

let nodes hashID (nodeMailbox:Actor<NodeMessage>) =
    let nodeName = nodeMailbox.Self.Path.Name
    let nodeIdx = nodeName.Substring(4) |> int
    let mutable nodeID = hashID
    //printfn "[%s] %s" nodeName (nodeMailbox.Self.Path.ToString())
    let mutable leafSet = Map.empty
    let mutable neighborSet = Map.empty
    //let routingTable = Array2D.create (ceil(Math.Log((numNodes|>float),(rFactor|>float)))|>int) (rFactor-1) ""
    let routingTable = RoutingTable(nodeID, nodeIdx)
    //printfn "[%s] %d, %d, %A, %A, %A\n" nodeName xCoordinate yCoordinate leafSet neighborSet routingTable
    //printfn " tet = %d" (ceil(Math.Log((numNodes|>float),(rFactor|>float)))|>int)
    let rec loop() = actor {
        let! (msg: NodeMessage) = nodeMailbox.Receive()
        match msg with
            
            | PastryInit nodeAIdx -> (* nodeA is assigned by pastryboss according to proximity metric *)

                (* random a 128 bit number and hash it with MD5 to get a 128 bit nodeID for myself *)
                //let rnd = Random()
                //nodeID <- hash (getID rnd)
                printfn "[%s] My nodeID is %A" nodeName nodeID

                (* If nodeA is NULL, means it is the very first node in the pastry network, inform the pastry boss for completion *)
                if nodeAIdx = 0 then
                    selectActorByName nodePastryBoss <! AddComplete nodeName
                    return! loop()
                
                (* Request nodeA's neighbor set for initailzing my own neighbor set*)
                

                (* Send route message to nodeA, with "JOIN" type Msg *)
                let hopCount = 0
                selectActorByName (getNodeName nodeAIdx) <! Route (JOIN, nodeID, nodeIdx, hopCount)
                
                
                return! loop()

            | PastryInitDone nodeZIdx ->
                printfn "[%s] My nodeZ is Node%d" nodeName nodeZIdx
                
                (* Inform all nodes in my leaf set *)
                for KeyValue(leafIdx, nodeKey) in leafSet do
                    selectActorByName (getNodeName leafIdx) <! UpdateStates (nodeIdx, nodeID)
                (* Inform all nodes in my routing table *)
                 
                for i in 0 .. nrow-1 do
                    for j in 0 .. ncol-1 do
                        let rtIdx = routingTable.GetEntry(i,j).NodeIdx
                        if rtIdx <> -1 then
                            printfn "[%s] Inform to Node%d in routing table" nodeName rtIdx
                            selectActorByName (getNodeName rtIdx) <! UpdateStates (nodeIdx, nodeID)

                (* Inform all nodes in my neighbor set *)
                for KeyValue(nborIdx, nodeKey) in neighborSet do
                    selectActorByName (getNodeName nborIdx) <! UpdateStates (nodeIdx, nodeID)

                (* Notice the pastry boss that I am successfully added to the network *)
                //TODO: should wait for all the updates are complete?
                selectActorByName nodePastryBoss <! AddComplete nodeName
                return! loop()

            (* make sure the message sender would always be the origin "new Node", use message "forward" *)
            | Route (routeType, key, senderIdx, hopCount) ->
                match (routeType) with
                    | JOIN ->
                        let senderNodeName = nodeMailbox.Sender().Path.Name
                        let senderNodeIdx = senderNodeName.Substring(4) |> int
                        printfn "[%s] Receive JOIN route from \"%s\"" nodeName senderNodeName
                        (* if I am the nodeA, send InitNborSet message to the new join noew *)
                        if hopCount = 0 then
                            selectActorByName (getNodeName senderIdx) <! InitNborSet neighborSet
                        //check if it is in the range of leafset
                        let mutable nextNodeIdx = getSmallestDiffLeafNode leafSet key nodeID nodeIdx
                        if nextNodeIdx <> 0 then
                            // forward JOIN msg to this leaf node
                            
                            //printfn "[%s] NodeZ found which is Node%d" nodeName nodeZIdx
                            if nextNodeIdx <> nodeIdx then
                                //nextNodeName <- nodeNamePrefix + nodeZIdx.ToString()
                                //TODO: send my (hopcount) th row of routing table to sender
                                selectActorByName (getNodeName senderIdx) <! InitRoutingTable (hopCount,routingTable)
                                selectActorByName (getNodeName nextNodeIdx) <! Route (routeType, key, senderIdx, hopCount+1)
                                nodeMailbox.Self.Tell (UpdateStates (senderIdx, key))
                                return! loop()
                            else
                                // I am the nodeZ then, send my leafnode set to the joining node, tell it that nodeZ is found
                                selectActorByName (getNodeName senderIdx) <! InitLeafSet leafSet
                                selectActorByName (getNodeName senderIdx) <! PastryInitDone nextNodeIdx
                                nodeMailbox.Self.Tell (UpdateStates (senderIdx, key))
                                return! loop()
                        else
                            // the common prefix length between key and present nodeID
                            //printfn "key:%s, nodeID:%s" key nodeID
                            let prefixLen = shl key nodeID
                            let i = prefixLen
                            let j = hexCharToInt key.[i]
                            
                            if i < nrow && j < hashedIdLength && routingTable.Table.[i,j].NodeID <> "" then
                                //DONE: use routing table here, to find a nodeIdx to send the join route meassage
                                nextNodeIdx <- routingTable.Table.[i,j].NodeIdx
                                printfn "[%s] Routing Table, route \"Node%d join\"to Node%d, hopcount:%d" nodeName senderIdx nextNodeIdx hopCount
                                selectActorByName (getNodeName senderIdx) <! InitRoutingTable (hopCount,routingTable)
                                selectActorByName (getNodeName nextNodeIdx) <! Route (routeType, key, senderIdx, hopCount+1)
                                nodeMailbox.Self.Tell (UpdateStates (senderIdx, key))
                                return! loop()
                            else
                                // got through all the nodes in state tables 
                                //such that shl(nodeID, key) >= prefixLen and |nodeID - key| < |present nodeID - key|
                                
                                //let mutable tmpNodeIdx = 0
                                let mutable endOfLoop = false
                                (* Search in leaf set *)
                                for KeyValue(leafIdx, nodeKey) in leafSet do
                                    if (shl nodeKey key) >= prefixLen && (diffNumeric nodeKey key) < (diffNumeric nodeID key) then
                                        nextNodeIdx <- leafIdx
                                    // if the nextNodeIdx is the sender itself, then don't route it back to the sender
                                    if nextNodeIdx <> 0 && nextNodeIdx <> senderIdx && not endOfLoop then
                                        endOfLoop <- true
                                        printfn "[%s] Rare case, find in leaf, route \"Node%d join\"to Node%d, hopcount:%d" nodeName senderIdx nextNodeIdx hopCount
                                        selectActorByName (getNodeName senderIdx) <! InitRoutingTable (hopCount,routingTable)
                                        selectActorByName (getNodeName nextNodeIdx) <! Route (routeType, key, senderIdx, hopCount+1)
                                        nodeMailbox.Self.Tell (UpdateStates (senderIdx, key))
                                        return! loop()
                                if endOfLoop then
                                    return! loop()

                                (* Search in routing table *)
                                for i in 0..nrow-1 do
                                    for j in 0..ncol-1 do
                                        let rtKey = routingTable.GetEntry(i,j).NodeID
                                        let rtIdx = routingTable.GetEntry(i,j).NodeIdx
                                        if rtKey <> "" && rtIdx <> -1 && (shl rtKey key) >= prefixLen && (diffNumeric rtKey key) < (diffNumeric nodeID key) then
                                            nextNodeIdx <- rtIdx
                                        if nextNodeIdx <> 0 && nextNodeIdx <> senderIdx && not endOfLoop then
                                            endOfLoop <- true
                                            printfn "[%s] Rare case, find in routingT, route \"Node%d join\"to Node%d, hopcount:%d" nodeName senderIdx nextNodeIdx hopCount
                                            selectActorByName (getNodeName senderIdx) <! InitRoutingTable (hopCount,routingTable)
                                            selectActorByName (getNodeName nextNodeIdx) <! Route (routeType, key, senderIdx, hopCount+1)
                                            nodeMailbox.Self.Tell (UpdateStates (senderIdx, key))
                                            return! loop()
                                if endOfLoop then
                                    return! loop()

                                (* Search in neighbor set*)
                                for KeyValue(nborIdx, nodeKey) in neighborSet do
                                    if (shl nodeKey key) >= prefixLen && (diffNumeric nodeKey key) < (diffNumeric nodeID key) then
                                        nextNodeIdx <- nborIdx
                                    if nextNodeIdx <> 0 && nextNodeIdx <> senderIdx && not endOfLoop then
                                        endOfLoop <- true
                                        printfn "[%s] Rare case, find in nbor, route \"Node%d join\"to Node%d, hopcount:%d" nodeName senderIdx nextNodeIdx hopCount
                                        selectActorByName (getNodeName senderIdx) <! InitRoutingTable (hopCount,routingTable)
                                        selectActorByName (getNodeName nextNodeIdx) <! Route (routeType, key, senderIdx, hopCount+1)
                                        nodeMailbox.Self.Tell (UpdateStates (senderIdx, key))
                                        return! loop()
                                if endOfLoop then
                                    return! loop()
                                
                                (* If there is no any node found, then I will be the nodeZ, because there are too less nodes in the network*)
                                printfn "[%s] Failed to use routing algorithm, hopCount:%d" nodeName hopCount
                                selectActorByName (getNodeName senderIdx) <! InitLeafSet leafSet
                                selectActorByName (getNodeName senderIdx) <! PastryInitDone nodeIdx
                                nodeMailbox.Self.Tell (UpdateStates (senderIdx, key))
                                return! loop()
                                
                        printfn "[%s] ajsoidfjoeijfowiejfi" nodeName
                        Environment.Exit 1
                        return! loop()
                        
                printfn "[%s] [ROUTE] [JOIN] Error!" nodeName
                Environment.Exit 1  
                return! loop()
            | InitLeafSet newLeafSet ->
                //initial my leafSet, is this right? Copy the newMap to local map?
                printfn "[%s] InitLeafSet from %s" nodeName (nodeMailbox.Sender().Path.Name)
                leafSet <- newLeafSet
                return! loop()
            | InitNborSet newNborSet ->
                printfn "[%s] InitNborSet from %s" nodeName (nodeMailbox.Sender().Path.Name)
                neighborSet <- newNborSet
                return! loop()
            | InitRoutingTable (row, newRTB)->
                // update my routing table row i according this row number
                routingTable.InitWith row newRTB
                return! loop()
            | UpdateStates (newNodeIdx, newNodeID) ->
                printfn "[%s] UpdateStates for Node%d" nodeName newNodeIdx
                // update my own state tables according this new joined node
                leafSet <- updateLeafSet nodeID leafSet newNodeIdx newNodeID
                
                //printfn "[%s][Update leaf set]\n %A" nodeName leafSet
                neighborSet <- updateNeighborSet nodeIdx neighborSet newNodeIdx newNodeID
                //printfn "[%s][Update nbor set]\n %A" nodeName neighborSet
                (* update my own routing table *)
                routingTable.Update (newNodeID, newNodeIdx)
                //TODO:
                return! loop()
        return! loop()
    }
    loop()

let pastryBoss numNodes (pbossMailbox:Actor<PastryNodeMessage>) =
    let nodeName = pbossMailbox.Self.Path.Name
    //printfn "[%s] %s" nodeName (nodeMailbox.Self.Path.ToString())
    //printfn "[%s]\n %A\n" nodeName proxMetric
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
                let rnd = Random()
                let hashID = (hash (getID rnd))
                spawn system newNodeName (nodes hashID) |> ignore
                printfn "[%s] Start to add \"%s\" to the pastry network\n" nodeName newNodeName
                
                
                (* find a closest node in the network according to the proximity metric *)
                //let rnd = Random()
                //for i in 1 .. 5 do
                //    networkNodeSet <- networkNodeSet.Add(rnd.Next(1,numNodes))
                let mutable nodeAIdx = 0
                if not networkNodeSet.IsEmpty then
                    let mutable distance = maxDistance + 1
                    for idx in networkNodeSet do
                        //let row = (min idx nodeCount)-1 //array starts from 0
                        //let col = (max idx nodeCount)-1 //array starts from 0
                        //printfn "node:%d, row:%d, col:%d nodeA:%s dis:%d\n" node row col nodeA distance
                        //let prox = getProx proxMetric idx nodeCount
                        let prox = proxMetric.GetDistance idx nodeCount
                        if prox < distance then
                            nodeAIdx <- idx
                            distance <- prox
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
                printfn "fasdf The whole pastry network has been built, total node counts: %d\n" nodeCount
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
        let numNodes = argv.[0] |> int
        let numRequest = argv.[1] |> int
        
        //test <- ProxityMetric (tmpNumNodes, maxDistance)
        //test.ShowAll
        
        
        
        // start to build the pastry network, ofcourse at least one
        (* get the input arguments *)
        //numNodes <- tmpNumNodes
        proxMetric <- ProxityMetric(numNodes, maxDistance)
        //printfn "%d ,,,, %d" (proxMetric.GetDistance 29 2) (proxMetric.GetDistance 2 29)
        //Environment.Exit 1
        printfn "[MAIN] Start, numNodes:%d, numReqquest:%d" numNodes numRequest
        if numNodes < 0 then
            printfn "numNodes should bigger than zero"
            Environment.Exit 1

        (* generate proximity metric *)
        (*
        let proxMetric = Array2D.create numNodes numNodes 0
        
        for i in 1 .. numNodes do
            for j in (i) .. (numNodes-1) do
                let rnd = Random()
                proxMetric.[i-1,j] <- rnd.Next(0, maxDistance)
        *)
        //printfn "%A\n" proxMetric

        (* Let pastry boss to help building the pastry network *)
        let pastryBossRef = spawn system nodePastryBoss (pastryBoss numNodes)
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
