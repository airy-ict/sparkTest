package com.qzt360.shanDong
import org.apache.spark.{ SparkConf, SparkContext }

object SparkShellSD {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("SparkShell")
    val sc = new SparkContext(sparkConf)
    val dpi = sc.textFile("/user/wlan/dpi/unat/current/*20160326*");
    val dpiwh = dpi.filter(_.contains("218.56.174.")) union dpi.filter(_.contains("218.56.175."))
    dpiwh.count

    val dpicur = sc.textFile("/user/wlan/dpi/unat/current/*2016032*")
    val dpicurwh = dpicur.filter(_.contains("218.56.174.")) union dpicur.filter(_.contains("218.56.175."))
    val dpihis = sc.textFile("/user/wlan/dpi/unat/history/*2016032*/*")

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

    val nat = sc.textFile("/user/wlan/nat/history/*.ok")
    val lanIp = nat.map(line => {
      val lines = line.split("\\s+")
      lines(2)
    }).distinct
    lanIp.collect
    lanIp.saveAsTextFile("/user/zhaogj/lanIp")
    val natIp = nat.map(line => {
      val lines = line.split("\\s+")
      lines(6)
    }).distinct
    natIp.collect
    natIp.saveAsTextFile("/user/zhaogj/natIp")
    lanIp.distinct.saveAsTextFile("")
  }
}