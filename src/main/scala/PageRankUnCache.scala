/**
 * Created by z on 12/11/15.
 */

package org.zork.graphx

import org.apache.spark.Logging
import org.apache.spark.graphx._

import scala.reflect.ClassTag


object PageRankUnCache extends Logging{

  def run[VD: ClassTag, ED: ClassTag](graph : Graph[VD, ED], numIter : Int,
    resetProb : Double = 0.15) : Graph[Double, Double] = {

    // initialize pagerankGraph with each edge attribute
    // having weight 1 / outDegree and each vertex with attribute 1.0
    var rankGraph: Graph[Double, Double] = graph
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
      .mapTriplets(e => 1.0 / e.srcAttr, TripletFields.Src)
      .mapVertices((id, attr) => resetProb)


    var iteration = 0

    while(iteration < numIter) {
      val rankUpdates = rankGraph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr),
        _ + _,
        TripletFields.Src
      )

      rankGraph = rankGraph.joinVertices(rankUpdates) {
        (id, oldRank, msgSum) => resetProb + (1.0 - resetProb) * msgSum
      }.cache()

      rankGraph.edges.foreachPartition(x=>{}) // materialize rankGraph.vertices
      logInfo(s"PageRank finished iteration $iteration")

      iteration += 1
    }

    rankGraph
  }



}
