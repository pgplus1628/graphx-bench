/**
 * Created by z on 12/14/15.
 */

package org.zork.graphx

import java.io.Serializable

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.spark.Logging
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.util.Random

object SVDPPBench extends Logging {


  class Conf (
    var rank: Int,
    var maxIters : Int,
    var minVal : Double,
    var maxVal : Double,
    var gamma1 : Double,
    var gamma2 : Double,
    var gamma6 : Double,
    var gamma7 : Double) extends Serializable


  def run(edges : RDD[Edge[Double]], conf : Conf) : Long =
  {
    // Generate default vertex attribute
    def defaultF(rank: Int): (Array[Double], Array[Double], Double, Double) = {
      // TODO: use a fixed random seed
      val v1 = Array.fill(rank)(Random.nextDouble())
      val v2 = Array.fill(rank)(Random.nextDouble())
      (v1, v2, 0.0, 0.0)
    }

    // calculate global rating mean
    edges.cache()
    val (rs, rc) = edges.map(e => (e.attr, 1L)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    val u = rs / rc


    // construct graph
    var g = Graph.fromEdges(edges, defaultF(conf.rank)).cache()
    materialize(g)
    edges.unpersist()

    // Calculate initial bias and norm
    val t0 = g.aggregateMessages[(Long, Double)](
      ctx => { ctx.sendToSrc((1L, ctx.attr)); ctx.sendToDst((1L, ctx.attr)) },
      (g1, g2) => (g1._1 + g2._1, g1._2 + g2._2))

    val gJoinT0 = g.outerJoinVertices(t0) {
      (vid: VertexId, vd: (Array[Double], Array[Double], Double, Double),
       msg: Option[(Long, Double)]) =>
        (vd._1, vd._2, msg.get._2 / msg.get._1 - u, 1.0 / scala.math.sqrt(msg.get._1))
    }.cache()
    materialize(gJoinT0)
    g.unpersist()
    g = gJoinT0


    def sendMsgTrainF(conf: Conf, u: Double)
                     (ctx: EdgeContext[
                       (Array[Double], Array[Double], Double, Double),
                       Double,
                       (Array[Double], Array[Double], Double)]) {
      val (usr, itm) = (ctx.srcAttr, ctx.dstAttr)
      val (p, q) = (usr._1, itm._1)
      val rank = p.length
      var pred = u + usr._3 + itm._3 + blas.ddot(rank, q, 1, usr._2, 1)
      pred = math.max(pred, conf.minVal)
      pred = math.min(pred, conf.maxVal)
      val err = ctx.attr - pred
      // updateP = (err * q - conf.gamma7 * p) * conf.gamma2
      val updateP = q.clone()
      blas.dscal(rank, err * conf.gamma2, updateP, 1)
      blas.daxpy(rank, -conf.gamma7 * conf.gamma2, p, 1, updateP, 1)
      // updateQ = (err * usr._2 - conf.gamma7 * q) * conf.gamma2
      val updateQ = usr._2.clone()
      blas.dscal(rank, err * conf.gamma2, updateQ, 1)
      blas.daxpy(rank, -conf.gamma7 * conf.gamma2, q, 1, updateQ, 1)
      // updateY = (err * usr._4 * q - conf.gamma7 * itm._2) * conf.gamma2
      val updateY = q.clone()
      blas.dscal(rank, err * usr._4 * conf.gamma2, updateY, 1)
      blas.daxpy(rank, -conf.gamma7 * conf.gamma2, itm._2, 1, updateY, 1)
      ctx.sendToSrc((updateP, updateY, (err - conf.gamma6 * usr._3) * conf.gamma1))
      ctx.sendToDst((updateQ, updateY, (err - conf.gamma6 * itm._3) * conf.gamma1))
    }


    val start_ms = System.currentTimeMillis()
    println("SVDPP Start : " + start_ms)

    for (i <- 0 until conf.maxIters) {
      // Phase 1, calculate pu + |N(u)|^(-0.5)*sum(y) for user nodes
      g.cache()
      val t1 = g.aggregateMessages[Array[Double]](
        ctx => ctx.sendToSrc(ctx.dstAttr._2),
        (g1, g2) => {
          val out = g1.clone()
          blas.daxpy(out.length, 1.0, g2, 1, out, 1)
          out
        })
      val gJoinT1 = g.outerJoinVertices(t1) {
        (vid: VertexId, vd: (Array[Double], Array[Double], Double, Double),
         msg: Option[Array[Double]]) =>
          if (msg.isDefined) {
            val out = vd._1.clone()
            blas.daxpy(out.length, vd._4, msg.get, 1, out, 1)
            (vd._1, out, vd._3, vd._4)
          } else {
            vd
          }
      }.cache()
      materialize(gJoinT1)
      g.unpersist()
      g = gJoinT1

      // Phase 2, update p for user nodes and q, y for item nodes
      g.cache()
      val t2 = g.aggregateMessages(
        sendMsgTrainF(conf, u),
        (g1: (Array[Double], Array[Double], Double), g2: (Array[Double], Array[Double], Double)) =>
        {
          val out1 = g1._1.clone()
          blas.daxpy(out1.length, 1.0, g2._1, 1, out1, 1)
          val out2 = g2._2.clone()
          blas.daxpy(out2.length, 1.0, g2._2, 1, out2, 1)
          (out1, out2, g1._3 + g2._3)
        })
      val gJoinT2 = g.outerJoinVertices(t2) {
        (vid: VertexId,
         vd: (Array[Double], Array[Double], Double, Double),
         msg: Option[(Array[Double], Array[Double], Double)]) => {
          val out1 = vd._1.clone()
          blas.daxpy(out1.length, 1.0, msg.get._1, 1, out1, 1)
          val out2 = vd._2.clone()
          blas.daxpy(out2.length, 1.0, msg.get._2, 1, out2, 1)
          (out1, out2, vd._3 + msg.get._3, vd._4)
        }
      }.cache()
      materialize(gJoinT2)
      g.unpersist()
      g = gJoinT2
    }


    val end_ms = System.currentTimeMillis()
    println("SVDPP End : " + end_ms)

    println("SVDPP Totoal: " + (end_ms - start_ms))

    end_ms - start_ms

  }


  private def materialize(g : Graph[_, _]) : Unit = {
    g.vertices.count()
    g.edges.count()
  }


}
