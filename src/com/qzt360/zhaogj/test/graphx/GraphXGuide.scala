package com.qzt360.zhaogj.test.graphx
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object GraphXGuide {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GraphXGuide")
    val sc = new SparkContext(conf)

    // Create an RDD for the vertices
    //    val users: RDD[(VertexId, (String, String))] =
    //      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
    //        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))

    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof")),
        (4L, ("peter", "student"))))

    // Create an RDD for edges
    //    val relationships: RDD[Edge[String]] =
    //      sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
    //        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"),
        Edge(4L, 0L, "student"), Edge(5L, 0L, "colleague")))

    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")

    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)

    graph.vertices.count
    // Count all users which are postdocs
    graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count

    graph.edges.count
    // Count all the edges where src > dst
    graph.edges.filter(e => e.srcId > e.dstId).count

    graph.edges.filter { case Edge(src, dst, prop) => src > dst }.count

    graph.triplets.count
    // Use the triplets view to create an RDD of facts.
    val facts: RDD[String] =
      graph.triplets.map(triplet =>
        triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
    facts.collect.foreach(println(_))

    // Notice that there is a user 0 (for which we have no information) connected to users
    // 4 (peter) and 5 (franklin).
    graph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1).collect.foreach(println(_))

    // Use the implicit GraphOps.inDegrees operator
    val inDegrees: VertexRDD[Int] = graph.inDegrees

    // Remove missing vertices as well as the edges to connected to them
    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")

    // The valid subgraph will disconnect users 4 and 5 by removing user 0
    validGraph.vertices.collect.foreach(println(_))

    validGraph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1).collect.foreach(println(_))

    // Run Connected Components
    val ccGraph = graph.connectedComponents() // No longer contains missing field
    // Restrict the answer to the valid subgraph
    val validCCGraph = ccGraph.mask(validGraph)
  }
}