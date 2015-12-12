/**
 * Created by z on 12/11/15.
 */

package org.zork.graphx

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

import scala.util.Random

import com.github.fommil.netlib.BLAS.{getInstance => blas}

object SGD extends Logging{

  class Conf(
      var rank : Int,
      var maxIters : Int,
      var minVal : Double,
      var maxVal : Double,
      var lambda1 : Double,
      var gamma1 : Double)
    extends Serializable

  def run(edges : RDD[Edge[Double]], conf: Conf)
    : Long =
  {

    // generate default vertex attributes
    def defaultF(rank :Int) : Array[Double] = {
      Array.fill(rank)(Random.nextDouble())
    }

    // construct graph
    edges.cache()
    var g = Graph.fromEdges(edges, defaultF(conf.rank)).cache()
    materialize(g)
    edges.unpersist()

    def sendGrad( ctx : EdgeContext[Array[Double],  Double, Array[Double] ]): Unit = {
      val (usr, itm) = (ctx.srcAttr, ctx.dstAttr)
      var pred = blas.ddot(conf.rank, usr, 1, itm, 1)
      pred = math.max(pred, conf.minVal)
      pred = math.min(pred, conf.maxVal)
      val err = ctx.attr - pred

      // update U
      val updateU = usr.clone()
      blas.dscal(conf.rank, -1.0 * conf.lambda1, updateU, 1)
      blas.daxpy(conf.rank, err, itm, 1, updateU, 1)
      blas.dscal(conf.rank, conf.gamma1, updateU, 1)

      // update V
      val updateV = itm.clone()
      blas.dscal(conf.rank, -1.0 * conf.lambda1, updateV, 1)
      blas.daxpy(conf.rank, err, usr, 1, updateV, 1)
      blas.dscal(conf.rank, conf.gamma1, updateV, 1)

      // send updateU to src
      ctx.sendToSrc(updateU)
      // send updateV to dst
      ctx.sendToDst(updateV)
    }


    val start_ms = System.currentTimeMillis()
    println("Start ms : " + start_ms)

    for ( i <- 0 until conf.maxIters) {
      g.cache()

      // gen grad for use and item, message auto combined
      val updates = g.aggregateMessages(
          sendGrad,
          (msg1 : Array[Double], msg2 : Array[Double] ) => {
            val out = msg1.clone()
            blas.daxpy(out.length, 1.0, msg2, 1, out, 1)
            out
          },
          TripletFields.All
      )

      val gNew = g.outerJoinVertices(updates) {
        (vid : VertexId, vd : Array[Double], msg : Option[Array[Double]]) => {
          val out = vd.clone()
          blas.daxpy(out.length, 1.0, msg.get, 1, out, 1)
          out
        }
      }
      gNew.cache()

      materialize(gNew)
      g.unpersist()
      g = gNew
    }

    val end_ms = System.currentTimeMillis()
    println("End ms : " + end_ms)
    println("Cost : " + (end_ms - start_ms))

    end_ms - start_ms
  }


  /*
   * Forces materialization of a Graph
   */
  private def materialize(g : Graph[_,_]) :Unit = {
    g.vertices.count()
    g.edges.count()
  }

}