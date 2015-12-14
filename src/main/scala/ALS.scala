/**
 * Created by z on 12/12/15.
 */

package org.zork.graphx

import org.apache.spark.Logging
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.netlib.util.intW

import scala.util.Random

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import com.github.fommil.netlib.LAPACK.{getInstance => lapack}


object ALS extends Logging {

  class Conf (
     var rank :Int,
     var maxIters : Int,
     var minVal : Double,
     var maxVal : Double,
     var lambda1 : Double
   ) extends Serializable


  def run(edges : RDD[Edge[Double]], conf : Conf)
    : Long =
  {
    // generate default vertex attributes
    def defaultF(rank : Int) : Array[Double] = {
      Array.fill(rank)(Random.nextDouble())
    }

    // construct graph
    edges.cache()
    var g = Graph.fromEdges(edges, defaultF(conf.rank)).cache()
    materialize(g)
    edges.unpersist()

    // regulate vector Double
    val regs : VertexRDD[Double] = g.outerJoinVertices(g.outDegrees){ (vid, vd, deg) => deg.getOrElse(0) * conf.lambda1 }.vertices.cache()

    /*
     * input :
     * fea : feature vecor
     * rat : rating
     * output :
     * vec : update vector with size d
     * mat : update matrix with size d * d, in row-major order
     */
    def gen_grad(fea : Array[Double], rat : Double) : (Array[Double], Array[Double]) = {
      val out1 = Array.fill(conf.rank)(0.0) // vec out
      blas.daxpy(out1.length, rat, fea, 1, out1, 1)
      val out2 = Array.fill(conf.rank * conf.rank)(0.0) // matrix out
      blas.dger(conf.rank, conf.rank, 1.0, fea, 1, fea, 1, out2, conf.rank)
      (out1, out2)
    }

    def solve(fea : Array[Double], vec : Array[Double], mat : Array[Double]) : Array[Double] = {
      val ret = fea.clone()
      val lapack_ipiv = new Array[Int](conf.rank)
      val lapack_work = new Array[Double](conf.rank)
      val lapack_info = new intW(0)
      lapack.dsysv("U", conf.rank, 1, mat, conf.rank, lapack_ipiv, ret, conf.rank, lapack_work, conf.rank, lapack_info)
      ret
    }


    val start_ms = System.currentTimeMillis()
    println("Start ms : " + start_ms)

    for ( iter <- 0 until conf.maxIters) {
      // --------------- use update item ----------------//
      // gen updates
      val itm_updates = g.aggregateMessages[(Array[Double], Array[Double])](
        ctx => ctx.sendToDst(gen_grad(ctx.srcAttr, ctx.attr)),
        (msg1, msg2) => {
          val out1 = msg1._1.clone()
          val out2 = msg1._2.clone()
          blas.daxpy(out1.length, 1.0, msg2._1, 1, out1, 1)
          blas.daxpy(out2.length, 1.0, msg2._2, 1, out2, 1)
          (out1, out2)
        }
      )
      // regulate mat
      .innerJoin(regs) {
        (vid, vd, reg) => {
          val tt = vd._2.clone()
          for (i <- 0 to (conf.rank - 1) ) {
            tt(i * conf.rank) += reg
          }
          (vd._1, tt)
        }
      }

      // apply updates
      g = g.joinVertices(itm_updates) {
        (vid, vd, u) => {
          solve(vd, u._1, u._2)
        }
      }

      // --------------- item update user ----------------//
      // gen updates
      val usr_updates = g.aggregateMessages[(Array[Double], Array[Double])](
        ctx => ctx.sendToSrc(gen_grad(ctx.dstAttr, ctx.attr)),
        (msg1, msg2) => {
          val out1 = msg1._1.clone()
          val out2 = msg1._2.clone()
          blas.daxpy(out1.length, 1.0, msg2._1, 1, out1, 1)
          blas.daxpy(out2.length, 1.0, msg2._2, 1, out2, 1)
          (out1, out2)
        }
      )
        // regulate mat
        .innerJoin(regs) {
        (vid, vd, reg) => {
          val tt = vd._2.clone()
          for (i <- 0 to (conf.rank - 1) ) {
            tt(i * conf.rank) += reg
          }
          (vd._1, tt)
        }
      }

      // apply updates
      g = g.joinVertices(usr_updates) {
        (vid, vd, u) => {
          solve(vd, u._1, u._2)
        }
      }

      materialize(g)
    }

    val end_ms = System.currentTimeMillis()
    println("End ms : " + end_ms)

    end_ms - start_ms
  }

  private def materialize(g : Graph[_,_]) : Unit = {
    g.vertices.count()
    g.edges.count()
  }

}
