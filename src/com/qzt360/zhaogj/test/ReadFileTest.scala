package com.qzt360.zhaogj.test
import org.apache.spark.{ SparkConf, SparkContext }
/**
 * 读入文件为大量小文件时的测试
 */
object ReadFileTest {
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("<input> <output>")
      System.exit(0)
    }
    val sparkConf = new SparkConf().setAppName("ReadFileTest")
    val sc = new SparkContext(sparkConf)
    //val nat = sc.textFile(args(0)).coalesce(args(2).toInt, false)
    val nat = sc.textFile(args(0), args(2).toInt)
    nat.filter { x => x.contains("10.10") }.saveAsTextFile(args(1))
    sc.stop()
  }
}