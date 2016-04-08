package com.qzt360.zhaogj.test.rdd
import org.apache.spark.{ SparkConf, SparkContext }
/**
 * 顾名思义，reduceByKey就是对元素为KV对的RDD中Key相同的元素的Value进行reduce，因此，Key相同的多个元素的值被reduce为一个值，然后与原RDD中的Key组成一个新的KV对。
 */
object ReduceByKeyTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ReduceByKeyTest")
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
        for (e <- x) yield (e, 1)
      }
    }
    wordsRDD.collect()
    //x,y代表前后相邻元素的value
    val result = wordsRDD.reduceByKey((x, y) => x + y)
    result.collect()
    sc.stop()
  }
}