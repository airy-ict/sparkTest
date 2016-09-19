package com.qzt360.zhaogj.test.graphx
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object GraphXNetid {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GraphXGuide")
    val sc = new SparkContext(conf)

    val users: RDD[(VertexId, String)] = sc.textFile("/Users/zhaogj/work/datasets/graphx/netid.txt").map { x =>
      {
        val parts = x.split("\t")
        (parts(0).toLong, parts(1))
      }
    }
    val relationships: RDD[Edge[Int]] = sc.textFile("/Users/zhaogj/work/datasets/graphx/relationships.txt").map { x =>
      {
        val parts = x.split("\t")
        Edge(parts(0).toLong, parts(1).toLong, parts(2).toInt)
      }
    }
    
    val graph = Graph(users, relationships)
    graph.vertices.count
    graph.edges.count
    graph.triplets.count
    // Use the implicit GraphOps.inDegrees operator
    val inDegrees: VertexRDD[Int] = graph.inDegrees
    inDegrees.collect
    val outDegrees: VertexRDD[Int] = graph.outDegrees
    outDegrees.collect

    val tmp: VertexRDD[(String)] = graph.aggregateMessages[(String)](
      triplet => {
        triplet.sendToDst(triplet.srcAttr)
      },
      (a, b) => (a + "," + b))
    tmp.collect.foreach(println(_))
    
    graph.collectNeighbors(EdgeDirection.Out).collect.foreach(println(_))
  }
}