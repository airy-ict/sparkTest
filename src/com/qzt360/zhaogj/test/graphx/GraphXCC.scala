package com.qzt360.zhaogj.test.graphx
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object GraphXCC {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GraphXGuide")
    val sc = new SparkContext(conf)

    val graph = GraphLoader.edgeListFile(sc, "/Users/zhaogj/work/datasets/graphx/relationships.txt")
    val cc = graph.connectedComponents().vertices
    val users = sc.textFile("/Users/zhaogj/work/datasets/graphx/netid.txt").map { line =>
      val fields = line.split("\t")
      (fields(0).toLong, fields(1))
    }

    val ccByUsername = users.join(cc).map {
      case (id, (username, cc)) => (username, cc)
    }
    println(ccByUsername.collect().mkString("\n"))
    ccByUsername.map { case (username, userid) => (userid, username) }.reduceByKey(_ + "," + _).collect

  }
}