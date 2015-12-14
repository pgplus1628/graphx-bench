/**
 * Created by z on 12/12/15.
 */

import org.apache.spark.Logging
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

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

    //val reg : VertexRDD[Double] = g.outDegrees.map( (vid, vd) =>  )

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

//    def solve(fea : Array[Double], vec : Array[Double], mat : Array[Double]) : Array[Double] = {

//    }



    for ( iter <- 0 until conf.maxIters) {
      //------- user update item --------------//
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
//      .join(g.outDegrees) {
//        (vid : VertexId, vd : (Array[Double], Array[Double]), deg : Int)
//      }


//      val gJoinItm = g.outerJoinVertices(itm_updates) {
//        (vid : VertexId, vd : (Array[Double]), msg : (Array[Double], Array[Double])) => {

//        }
//      }

      // item update user

    }



    0
  }

  private def materialize(g : Graph[_,_]) : Unit = {
    g.vertices.count()
    g.edges.count()
  }

}
