package com.qzt360.zhaogj
import org.apache.spark.SparkContext

object TopK {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext
    val inputRDD = sc.textFile(args(0))
    val wordsRDD = inputRDD.flatMap(_.split("\\s+"))
    val wordCounts = wordsRDD.map(x => (x, 1)).reduceByKey(_ + _)
    val sortedRDD = wordCounts.map {
      case (key, value) => (value, key);
    }.sortByKey(true, 1)
    val topK = sortedRDD.top(args(1).toInt)
    topK.foreach(println)
  }
}