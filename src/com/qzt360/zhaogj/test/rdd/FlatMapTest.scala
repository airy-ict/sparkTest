package com.qzt360.zhaogj.test.rdd
import org.apache.spark.{ SparkConf, SparkContext }
/**
 * FlatMapTest的操作示例
 * 与map类似，区别是原RDD中的元素经map处理后只能生成一个元素，而原RDD中的元素经flatmap处理后可生成多个元素来构建新RDD。
 */
object FlatMapTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("FlatMapTest")
    val sc = new SparkContext(conf)
    /**
     * testData eg:
     * Everything will be OK.
     * Good good study, day day up.
     * Bye.
     */
    //文件变成一个RDD
    val fileRDD = sc.textFile("/user/zhaogj/input/testData")
    //朴素的写法
    val wordsRDDOld = fileRDD.map(_.split("\\s+")).flatMap { x =>
      {
        for (i <- 0 until x.length - 1) yield (x(i), 1)
      }
    }
    //简洁的写法
    val wordsRDD = fileRDD.map(_.split("\\s+")).flatMap { x =>
      {
        for (e <- x) yield (e, 1)
      }
    }
    /**
     *
     * yield的用法：
     * for 循环中的 yield 会把当前的元素记下来，保存在集合中，循环结束后将返回该集合。
     * Scala 中 for 循环是有返回值的。如果被循环的是 Map，返回的就是  Map，被循环的是 List，返回的就是 List，以此类推。
     * 下面是摘自 《Programming in Scala》关于 yield 的解释:
     * For each iteration of your for loop, yield generates a value which will be remembered.
     * It's like the for loop has a buffer you can't see, and for each iteration of your for loop, another item is added to that buffer.
     * When your for loop finishes running, it will return this collection of all the yielded values.
     * The type of the collection that is returned is the same type that you were iterating over, so a Map yields a Map, a List yields a List, and so on.
     * Also, note that the initial collection is not changed; the for/yield construct creates a new collection according to the algorithm you specify.
     */
    wordsRDD.collect()
    wordsRDD.reduceByKey(_ + _).collect()

    sc.stop()
  }
}