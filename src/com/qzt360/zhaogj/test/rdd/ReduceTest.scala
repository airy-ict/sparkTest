package com.qzt360.zhaogj.test.rdd
import org.apache.spark.{ SparkConf, SparkContext }
/**
 * reduce将RDD中元素两两传递给输入函数，同时产生一个新的值，新产生的值与RDD中下一个元素再被传递给输入函数直到最后只有一个值为止。
 */
object ReduceTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ReduceTest")
    val sc = new SparkContext(conf)
    /**
     * testData eg:
     * Everything will be OK.
     * Good good study, day day up.
     * Bye.
     */
    //文件变成一个RDD
    val fileRDD = sc.textFile("/user/zhaogj/input/testData")
    //简洁的写法
    val wordsRDD = fileRDD.map(_.split("\\s+")).flatMap { x =>
      {
        for (e <- x) yield (e)
      }
    }
    wordsRDD.collect()
    val result = wordsRDD.reduce((x, y) => x + y)
    println(result)
    sc.stop()
  }
}