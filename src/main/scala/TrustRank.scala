/**
 * Created by z on 12/10/15.
 */

package org.zork.graphx

import org.apache.spark.Logging
import org.apache.spark.graphx._

import scala.reflect.ClassTag
import scala.util.Random


object TrustRank extends Logging {

  /*
   * VD : (double, double) denotes rank and score
   * ED : double , not used
   */
  def run[ VD : ClassTag, ED : ClassTag ] (
    graph: Graph[VD, ED], numIter : Int) : Long =
  {
    val resetProb : Double = 0.15
    val resetRank : Double = 0.15
    def resetScore : Double = Random.nextDouble()


    // initialize the rank and score with each edge attribute having
    // weight 1 / outDegree and each vertex with attribute (1.0, rand)
    var rankGraph : Graph[Double, Double] = graph
    // associted the degree with each vertex
    .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0)}
    // set the weight on the edges based on the degree
    .mapTriplets( e => 1.0/e.srcAttr, TripletFields.Src)
    // set the vertex attributes to the initial rank and score values
    .mapVertices( (id, attr) => resetRank)


    val scores = graph.vertices.map( v => (v._1, resetScore ) ).cache()

    var iteration = 0

    val start_ms = System.currentTimeMillis()
    println("Start time : " + start_ms)

    while(iteration < numIter) {
      //rankGraph.cache()

      // send rank value / outDegree to  dst vertices
      val rankUpdates = rankGraph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr), _ + _, TripletFields.Src)

      // update rank
      rankGraph = rankGraph.joinVertices(rankUpdates) {
        (id, oldRank, msgSum) => (1.0 - resetProb) * msgSum
      }.joinVertices(scores) {
        (id, oldRank, score) => oldRank + resetProb * score
      }
      //.cache()
      rankGraph.vertices.count() // materialize rank graph
      logInfo(s"TrustRank finished iteration $iteration.")

      iteration += 1
    }

    var end_ms = System.currentTimeMillis()
    println("End time : " + end_ms)

    println("Cost : " + (end_ms - start_ms))

    end_ms - start_ms
  }

}