package com.qzt360.zhaogj.test.rdd
import org.apache.spark.{ SparkConf, SparkContext }
/**
 * Filter的操作示例
 */
object FilterTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("FilterTest")
    val sc = new SparkContext(conf)
    /**
     * flatMapData eg:
     * Everything will be OK.
     * Good good study, day day up.
     * Bye.
     */
    //文件变成一个RDD
    val fileRDD = sc.textFile("/user/zhaogj/input/flatMapData")
    //朴素写法
    val findRDDOld = fileRDD.filter { x => x.contains("good") }
    //简洁写法
    val findRDD = fileRDD.filter(_.contains("good"))
    findRDD.collect
  }
}