package com.qzt360.shanDong
import org.apache.spark.{ SparkConf, SparkContext }

object GetUrlTopK {
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("<inDpiLog> <outUrlCount> <partition>")
      System.exit(0)
    }
    val sparkConf = new SparkConf().setAppName("GetUrlTopK")
    val sc = new SparkContext(sparkConf)
    val dpi = sc.textFile(args(0)).coalesce(args(2).toInt, false)
    val urlCount = dpi.map(line => {
      val lines = line.split("\\s+")
      if (lines.length > 2) {
        (lines(2), 1)
      } else {
        ("errline", 1)
      }
    }).combineByKey(
      (v: Int) => v,
      (c: Int, v: Int) => c + v,
      (c1: Int, c2: Int) => c1 + c2)
    val urlCountSort = urlCount.sortByKey(false)
    urlCountSort.map(x => (x._1, x._2)).saveAsTextFile(args(1))
    sc.stop()
  }
}