package com.qzt360.zhaogj
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SparkReadFileTest {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("SparkReadFileTest")
    val sc = new SparkContext(sparkConf)
    val dpi = sc.textFile("/user/zhaogj/input/dpiB.txt")
    val wordCount = dpi.map(line => {
      line.length()
    })
    wordCount.saveAsTextFile(args(0))
  }
}