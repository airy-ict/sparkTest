package com.qzt360.zhaogj.test.rdd
import org.apache.spark.{ SparkConf, SparkContext }
/**
 * Map的操作示例
 * map是对RDD中的每个元素都执行一个指定的函数来产生一个新的RDD。任何原RDD中的元素在新RDD中都有且只有一个元素与之对应。
 */
object MapTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MapTest")
    val sc = new SparkContext(conf)
    /**
     * testData eg:
     * Everything will be OK.
     * Good good study, day day up.
     * Bye.
     */
    //文件变成一个RDD
    val fileRDD = sc.textFile("/user/zhaogj/input/testData")
    //对于一个文件来说，map处理的每个元素就是每行数据了
    val xmlRDD = fileRDD.map { x => "<line> " + x + " </line>" }
    xmlRDD.collect()

    val partsRDD = xmlRDD.map { x => x.split("\\s+") }
    partsRDD.collect()

    val lengthRDD = partsRDD.map { x => x.length }
    lengthRDD.collect()

    val wordRDD = fileRDD.map { line =>
      {
        val parts = line.split("\\s+")
        if (parts.length >= 2) {
          parts(1)
        } else {
          "errLine"
        }
      }
    }
    wordRDD.collect()
    sc.stop()
  }
}