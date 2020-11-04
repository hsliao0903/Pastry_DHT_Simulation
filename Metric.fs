namespace Pastry

open System

module Metric =

    type ProxityMetric (numNodes:int, maxDistance:int) =
        let mutable pm = Map.empty
        let rand = Random()
        do for i in 1 .. numNodes do
            for j in (i+1) .. (numNodes) do
                let d = rand.Next(0,maxDistance)
                //printfn "(%d,%d) %d" i j d
                pm <- pm.Add((i, j),d)
                pm <- pm.Add((j, i),d)

        member val FullMetric = pm
        member this.GetDistance (i:int) (j:int) = 
            if i = -1 || j = -1 
            then 0
            else pm.[i,j]
        //member this.ShowAll =
        //    printfn "%A" pm
