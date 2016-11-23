/**
  * Created by zork on 22/11/2016.
  */

package org.zork.graphx

import org.apache.spark.Logging
import org.apache.spark.graphx._

import scala.reflect.ClassTag

object PageRankPregelConverge extends Logging {

  //tol : the tolerance allowed at convergence (smaller => more accurate).
  def run[VD: ClassTag, ED: ClassTag](graph : Graph[VD, ED], numIter : Int,
    resetProb : Double = 0.15, tol : Double = 0.1) : Long = {

    // initialize pagerankGraph with each edge attribute
    // having weight 1 / outDegree and each vertex with attribute 1.0
    var rankGraph: Graph[Double, Double] = graph
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
      .mapTriplets(e => 1.0 / e.srcAttr, TripletFields.Src)
      .mapVertices((id, attr) => resetProb)


    def vertexProgram(id :VertexId, attr: Double, msgSum : Double) : Double = {
      attr + ( 1 - resetProb) * msgSum
    }

    def sendMessage(edge :EdgeTriplet[Double, Double]) = {
      if (edge.srcAttr > tol) {
        Iterator((edge.dstId, edge.srcAttr * edge.attr))
      } else {
        Iterator.empty
      }
    }

    def messageCombiner(a : Double, b : Double) : Double = a + b

    val initialMessage = resetProb / (1.0 - resetProb)

    val time_ms = System.currentTimeMillis()

    Pregel(rankGraph, initialMessage, numIter, activeDirection = EdgeDirection.Out) (
      vertexProgram,
      sendMessage,
      messageCombiner)
      .mapVertices((vid, attr) => attr)

    rankGraph.mapVertices( (id,x) => {})


    System.currentTimeMillis() - time_ms
  }

}
