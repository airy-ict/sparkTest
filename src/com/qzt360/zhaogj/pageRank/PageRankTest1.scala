package com.qzt360.zhaogj.pageRank
import org.apache.spark._
object PageRankTest1 {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("SparkHBaseGetSerializableTest")
    val sc = new SparkContext(sparkConf)
    // 假定邻居页面的List存储为Spark objectFile
    val links = sc.objectFile[(String, Seq[String])]("/user/zhaogj/input/pageRank.txt")
      .partitionBy(new HashPartitioner(100))
      .persist()

    //设置页面的初始rank值为1.0
    var ranks = links.mapValues(_ => 1.0)

    //迭代10次
    for (i <- 0 until 10) {
      val contributions = links.join(ranks).flatMap {
        case (pageId, (links, rank)) =>
          //注意此时的links为模式匹配获得的值，类型为Seq[String]，并非前面读取出来的页面List
          links.map(dest => (dest, rank / links.size))
      }
      //简化了的rank计算公式
      ranks = contributions.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }
    ranks.saveAsTextFile("/user/zhaogj/output/pageRank")
  }
}