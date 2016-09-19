package com.qzt360.zhaogj.test
import org.apache.spark.{ SparkConf, SparkContext }

object WordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)
    val textRDD = sc.textFile("/user/zhaogj/input/wordCountData")
//    val result = textRDD.flatMap {
//      case (key, value) => value.toString().split("\\s+")
//    }.map(word => (word, 1)).reduceByKey(_ + _)

  }
}