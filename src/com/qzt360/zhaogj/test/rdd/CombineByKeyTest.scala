package com.qzt360.zhaogj.test.rdd
import org.apache.spark.{ SparkConf, SparkContext }

object CombineByKeyTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("FindUrlRank")
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
    val result = wordsRDD.combineByKey(
      (v: Int) => v,
      (c: Int, v: Int) => c + v,
      (c1: Int, c2: Int) => c1 + c2)
    result.collect()
    sc.stop()

    //超级榨汁机
    case class Fruit(kind: String, weight: Int) {
      def makeJuice: Juice = Juice(weight * 100)
    }
    case class Juice(volumn: Int) {
      def add(j: Juice): Juice = Juice(volumn + j.volumn)
    }
    val apple1 = Fruit("apple", 5)
    val apple2 = Fruit("apple", 8)
    val orange1 = Fruit("orange", 10)
    val fruit = sc.parallelize(List(("apple", apple1), ("orange", orange1), ("apple", apple2)))
    fruit.collect
    val juice = fruit.combineByKey(
      f => f.makeJuice,
      (j: Juice, f) => j.add(f.makeJuice),
      (j1: Juice, j2: Juice) => j1.add(j2))
    juice.collect
  }
}