package com.qzt360.zhaogj
import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext
    val inputRDD = sc.textFile(args(0))
    val wordsRDD = inputRDD.flatMap(_.split("\\s+"))
    val wordCounts = wordsRDD.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.saveAsTextFile(args(1))
  }
}