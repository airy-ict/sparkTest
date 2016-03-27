package com.qzt360.zhaogj
import org.apache.spark.SparkContext

object LineCount {
  def main(args: Array[String]): Unit = {
    println("begin LineCount")
    val sc = new SparkContext
    val dpiRDD = sc.textFile("hdfs:///user/zhaogj/input/")
    val weiboRDD = dpiRDD.filter(_.contains("weibo"))
    weiboRDD.saveAsTextFile("hdfs:///user/zhaogj/output")
    println("end LineCount")
  }
}