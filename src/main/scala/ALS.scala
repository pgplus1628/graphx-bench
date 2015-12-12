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


    def sendMsg(edge : EdgeTriplet[Array[Double], Double]) ={
      val f = edge.srcAttr // feature
      val r = edge.attr    // rating
      val out1 = Array.fill(conf.rank)(0.0) // feature vec out
      blas.daxpy(out1.length, r, f, 1, out1, 1)
      val out2 = Array.fill(conf.rank * conf.rank)(0.0) // mat out
      // TODO

      //Iterator((edge.dstId, ))
    }


//    def vprog(id : VertexId, attr : Array[Double], msgSum : Array[Double]): Array[Double] = {
//    }

    0
  }

  private def materialize(g : Graph[_,_]) : Unit = {
    g.vertices.count()
    g.edges.count()
  }

}
