package com.qzt360.zhaogj
import org.apache.spark.SparkContext

object SparkRDDAPITest {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext
    val a = sc.parallelize(1 to 9, 3)
    a.collect()

    //mapPartitions
    a.mapPartitions(myfunc).collect()
    //mapValues
    val b = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
    b.collect()
    val c = b.map(x => (x.length(), x))
    c.collect()
    c.mapValues("x" + _ + "x").collect()
    //mapWith
    val d = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3)
    d.collect()
    d.mapWith(a => a * 10)((a, b) => (b + 2)).collect()
    d.mapWith(a => a)((a, b) => (b)).collect()
    //flatMap
    val e = sc.parallelize(1 to 10, 5)
    e.collect()
    e.mapWith(a => a)((a, b) => (b)).collect()
    e.flatMap(x => List(x, x, x)).collect()

    //join
    //val f = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
    val f = sc.parallelize(List("a", "b", "c"), 2)
    f.collect
    val g = f.keyBy(_.length)
    g.collect
    //res25: Array[(Int, String)] = Array((3,dog), (6,salmon), (6,salmon), (3,rat), (8,elephant))
    //val h = sc.parallelize(List("dog", "cat", "gnu", "salmon", "rabbit", "turkey", "wolf", "bear", "bee"), 3)
    val h = sc.parallelize(List("d", "e", "f"), 2)
    h.collect
    val i = h.keyBy(_.length)
    i.collect
    //res26: Array[(Int, String)] = Array((3,dog), (3,cat), (3,gnu), (6,salmon), (6,rabbit), (6,turkey), (4,wolf), (4,bear), (3,bee))
    g.join(i).collect
    g.cogroup(i).collect
    //res65: Array[(Int, (Iterable[String], Iterable[String]))] = Array(
    //(6,(CompactBuffer(salmon, salmon),CompactBuffer(salmon, rabbit, turkey))), 
    //(3,(CompactBuffer(dog, rat),CompactBuffer(bee, dog, cat, gnu))), 
    //(4,(CompactBuffer(),CompactBuffer(wolf, bear))), 
    //(8,(CompactBuffer(elephant),CompactBuffer())))

    //res29: Array[(Int, (String, String))] = Array(
    //(6,(salmon,salmon)), (6,(salmon,rabbit)), (6,(salmon,turkey)), (6,(salmon,salmon)), (6,(salmon,rabbit)), (6,(salmon,turkey)), 
    //(3,(dog,bee)), (3,(dog,dog)), (3,(dog,cat)), (3,(dog,gnu)), (3,(rat,bee)), (3,(rat,dog)), (3,(rat,cat)), (3,(rat,gnu)))

    //res37: Array[(Int, (String, String))] = Array((1,(a,d)), (3,(ccc,fff)), (2,(bb,ee)))
    //res39: Array[(Int, (String, String))] = Array((2,(bb,ee)), (1,(a,d)), (3,(ccc,fff)))
    //res48: Array[(Int, (String, String))] = Array((1,(a,d)), (1,(a,e)), (1,(a,f)), (1,(b,d)), (1,(b,e)), (1,(b,f)), (1,(c,d)), (1,(c,e)), (1,(c,f))) 1*1
    //res45: Array[(Int, (String, String))] = Array((1,(b,d)), (1,(b,e)), (1,(b,f)), (1,(c,d)), (1,(c,e)), (1,(c,f)), (1,(a,d)), (1,(a,e)), (1,(a,f))) 2*2
    //res52: Array[(Int, (String, String))] = Array((1,(a,d)), (1,(a,e)), (1,(a,f)), (1,(b,d)), (1,(b,e)), (1,(b,f)), (1,(c,d)), (1,(c,e)), (1,(c,f))) 3*3

  }
  def myfunc[T](iter: Iterator[T]): Iterator[(T, T)] = {
    var res = List[(T, T)]()
    var pre = iter.next
    while (iter.hasNext) {
      val cur = iter.next;
      res.::=(pre, cur)
      pre = cur
    }
    res.iterator
  }
}