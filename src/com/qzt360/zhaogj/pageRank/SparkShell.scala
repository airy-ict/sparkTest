package com.qzt360.zhaogj.pageRank
import org.apache.spark.{ SparkConf, SparkContext }

object SparkShell {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("SparkShell")
    val sc = new SparkContext(sparkConf)

    val lines = sc.textFile("/user/zhaogj/input/pageRank.txt")

    val links = lines.map { s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()

    //    val links = ((lines.map { s =>
    //      val parts = s.split("\\s+")
    //      (parts(0), parts(1))
    //    }) union (lines.map { s =>
    //      val parts = s.split("\\s+")
    //      (parts(1), parts(0))
    //    })).distinct().groupByKey().cache()
    var ranks = links.mapValues(v => 1.0)

    for (i <- 1 to 100) {
      val contribs = links.join(ranks).values.flatMap {
        case (urls, rank) =>
          val size = urls.size
          urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }
    ranks.collect
  }
}