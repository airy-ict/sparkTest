package com.qzt360.shanDong
import org.apache.spark.{ SparkConf, SparkContext }

object GetLanNatIp {
  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println("<inNatLog> <outLanIp> <outNatIp> <partition>")
      System.exit(4)
    }
    val sparkConf = new SparkConf().setAppName("GetLanNatIp")
    val sc = new SparkContext(sparkConf)
    val nat = sc.textFile(args(0)).coalesce(args(3).toInt, false)
    //val nat = sc.textFile("/user/wlan/nat/history/*201602*.ok")
    nat.map(line => {
      val lines = line.split("\\s+")
      if (lines.length > 2) {
        (lines(2), 1)
      } else {
        ("errline", 1)
      }
    }).combineByKey(
      (v: Int) => v,
      (c: Int, v: Int) => c + v,
      (c1: Int, c2: Int) => c1 + c2 //,
      //args(3).toInt
      )
      .saveAsTextFile(args(1))

    nat.map(line => {
      val lines = line.split("\\s+")
      if (lines.length > 6) {
        (lines(6), 1)
      } else {
        ("errline", 1)
      }
    }).combineByKey(
      (v: Int) => v,
      (c: Int, v: Int) => c + v,
      (c1: Int, c2: Int) => c1 + c2 //,
      //args(3).toInt
      )
      .saveAsTextFile(args(2))

    //    nat.map(line => {
    //      val lines = line.split("\\s+")
    //      if (lines.length > 2) {
    //        (lines(2), 1)
    //      } else {
    //        ("errline", 1)
    //      }
    //    }).repartition(args(3).toInt).reduceByKey(_ + _).saveAsTextFile(args(1))

    //    nat.map(line => {
    //      val lines = line.split("\\s+")
    //      if (lines.length > 6) {
    //        (lines(6), 1)
    //      } else {
    //        ("errline", 1)
    //      }
    //    }).repartition(args(3).toInt).reduceByKey(_ + _).saveAsTextFile(args(2))
  }
}