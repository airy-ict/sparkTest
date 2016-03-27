package com.qzt360.zhaogj
import org.apache.spark.SparkContext

object Test {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext
    val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
    val b = a.keyBy(_.length)
    val c = sc.parallelize(List("dog", "cat", "gnu", "salmon", "rabbit", "turkey", "wolf", "bear", "bee"), 3)
    val d = c.keyBy(_.length)
    b.collect
    d.collect
    b.leftOuterJoin(d).collect
    b.rightOuterJoin(d).collect
    b.join(d).collect
    //res3: Array[(Int, String)] = Array((3,dog), (6,salmon), (6,salmon), (3,rat), (8,elephant))
    //res5: Array[(Int, String)] = Array((3,dog), (3,cat), (3,gnu), (6,salmon), (6,rabbit), (6,turkey), (4,wolf), (4,bear), (3,bee))

    //leftOuterJoin
    //res6: Array[(Int, (String, Option[String]))] = Array(
    //(6,(salmon,Some(salmon))), (6,(salmon,Some(rabbit))), (6,(salmon,Some(turkey))), (6,(salmon,Some(salmon))), (6,(salmon,Some(rabbit))), (6,(salmon,Some(turkey))), 
    //(3,(rat,Some(bee))), (3,(rat,Some(dog))), (3,(rat,Some(cat))), (3,(rat,Some(gnu))), (3,(dog,Some(bee))), (3,(dog,Some(dog))), (3,(dog,Some(cat))), (3,(dog,Some(gnu))), 
    //(8,(elephant,None)))

    //join
    //res0: Array[(Int, (String, String))] = Array(
    //(6,(salmon,salmon)), (6,(salmon,rabbit)), (6,(salmon,turkey)), (6,(salmon,salmon)), (6,(salmon,rabbit)), (6,(salmon,turkey)), 
    //(3,(dog,bee)), (3,(dog,dog)), (3,(dog,cat)), (3,(dog,gnu)), (3,(rat,bee)), (3,(rat,dog)), (3,(rat,cat)), (3,(rat,gnu)))

    //rightOutJoin
    //res1: Array[(Int, (Option[String], String))] = Array(
    //(6,(Some(salmon),salmon)), (6,(Some(salmon),rabbit)), (6,(Some(salmon),turkey)), (6,(Some(salmon),salmon)), (6,(Some(salmon),rabbit)), (6,(Some(salmon),turkey)), 
    //(3,(Some(dog),dog)), (3,(Some(dog),cat)), (3,(Some(dog),gnu)), (3,(Some(dog),bee)), (3,(Some(rat),dog)), (3,(Some(rat),cat)), (3,(Some(rat),gnu)), (3,(Some(rat),bee)), 
    //(4,(None,wolf)), (4,(None,bear)))
    val a1 = sc.parallelize(List(1, 2, 3), 3)
    a1.fold(0)(_ + _)
    val count = sc.parallelize(1 to 2000000000, 128).map { i =>
      val x = Math.random()
      val y = Math.random()
      if (x * x + y * y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / 2000000000)
  }
}