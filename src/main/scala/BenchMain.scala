/**
 * Created by z on 12/10/15.
 */

package org.zork.graphx

import java.util

import org.apache.spark.{SparkConf, SparkContext, Logging}
import org.apache.spark.SparkContext._
import org.apache.spark.graphx.{GraphLoader, PartitionStrategy, GraphXUtils}
import org.apache.spark.graphx.lib._
import org.apache.spark.graphx.PartitionStrategy._
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

import org.zork.graphx.Timer

object BenchMain extends Logging {

  def main(args : Array[String]) : Unit = {
    if (args.length < 3) {
      System.err.println(
        "Usage : BenchMain <app> <file> " +
          "--numEPart=<num_edge_partition> " +
          "--numIter=<num_iterations> " +
          "--partStrategy=<partitioin strategy> " +
          " [other options]"
      )
      System.err.println("Supported apps : \n" +
        "PageRank ");
      System.exit(1)
    }

    // resolve input
    val app = args(0)
    val fname = args(1)
    val optionsList = args.drop(2).map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => (opt -> v)
        case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }

    val options = mutable.Map(optionsList: _*)

    val conf = new SparkConf()
    GraphXUtils.registerKryoClasses(conf)
    val numEPart = options.remove("numEPart").map(_.toInt).getOrElse {
      println("Set the number of edge partitions using --numEPart.")
      sys.exit(1)
    }
    val partitionStrategy: Option[PartitionStrategy] = options.remove("partStrategy")
      .map(PartitionStrategy.fromString(_))
    val edgeStorageLevel = options.remove("edgeStorageLevel")
      .map(StorageLevel.fromString(_)).getOrElse(StorageLevel.MEMORY_ONLY)
    val vertexStorageLevel = options.remove("vertexStorageLevel")
      .map(StorageLevel.fromString(_)).getOrElse(StorageLevel.MEMORY_ONLY)


    val numIter = options.remove("numIter").map(_.toInt).getOrElse {
      println("Set the number of iterations to run using --numIter")
      sys.exit(1)
    }


    app match {
      case "pagerank" =>
        val outFname = options.remove("output").getOrElse("")
        val numIterOpt = options.remove("numIter").map(_.toInt)

        options.foreach {
          case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
        }

        println("========================================")
        println("                 PageRank               ")
        println("========================================")

        val sc = new SparkContext(conf.setAppName("PageRank(" + fname + ")"))

        val unpartitionedGraph = GraphLoader.edgeListFile(sc, fname,
          numEdgePartitions =  numEPart,
          edgeStorageLevel = edgeStorageLevel,
          vertexStorageLevel = vertexStorageLevel).cache()
        val graph = partitionStrategy.foldLeft(unpartitionedGraph)(_.partitionBy(_))

        println("GRAPHX: Number of vertices " + graph.vertices.count)
        println("GRAPHX: Number of edges " + graph.edges.count)

        val timer = new Timer
        timer.start()
        val pr = PageRank.run(graph, numIter).vertices.cache()
        timer.stop()

        println("GRAPHX: PageRank CONF::Iteration " + numIter + ".")
        println("GRAPHX: PageRank TIMING::Total " + timer.elapsed() + " ms.")

        if (!outFname.isEmpty) {
          logWarning("Saving pageranks of pages to " + outFname)
          pr.map { case (id, r) => id + "\t" + r }.saveAsTextFile(outFname)
        }

        sc.stop()

      case "trustrank" =>
        val outFname = options.remove("output").getOrElse("")
        val numIterOpt = options.remove("numIter").map(_.toInt)

        options.foreach {
          case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
        }

        println("========================================")
        println("                 TrustRank              ")
        println("========================================")

        val sc = new SparkContext(conf.setAppName("TrustRank(" + fname + ")"))

        val unpartitionedGraph = GraphLoader.edgeListFile(sc, fname,
          numEdgePartitions =  numEPart,
          edgeStorageLevel = edgeStorageLevel,
          vertexStorageLevel = vertexStorageLevel).cache()
        val graph = partitionStrategy.foldLeft(unpartitionedGraph)(_.partitionBy(_))

        println("GRAPHX: Number of vertices " + graph.vertices.count)
        println("GRAPHX: Number of edges " + graph.edges.count)

        val timer = new Timer
        timer.start()
        val pr = TrustRank.run(graph, numIter).vertices.cache()
        timer.stop()

        println("GRAPHX: TrustRank CONF::Iteration " + numIter + ".")
        println("GRAPHX: TrustRank TIMING::Total " + timer.elapsed() + " ms.")

        if (!outFname.isEmpty) {
          logWarning("Saving trustranks of pages to " + outFname)
          pr.map { case (id, r) => id + "\t" + r }.saveAsTextFile(outFname)
        }

        sc.stop()


      case _ =>
        println("Invalid app.")

    }
  }
}