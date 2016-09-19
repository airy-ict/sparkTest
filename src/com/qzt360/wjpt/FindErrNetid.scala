package com.qzt360.wjpt
import org.apache.spark.{ SparkConf, SparkContext }

object FindErrNetid {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("NetidGroup")
    val sc = new SparkContext(conf)
    //im分析
    val im = sc.textFile("/user/wjpt/im")
    val keyIm = im.filter { x =>
      {
        var result = false
        val parts = x.split("\t")
        if (parts.length == 19 || parts.length == 22) {
          if (parts(7).equals("1002")) {
            if (parts(10).length() >= 5 || parts(10).length() <= 11) {
              val regEx = "[0-9]+".r
              val someQQ = regEx.findFirstIn(parts(10)).toString
              if (someQQ.length() >= 11) {
                if (someQQ.substring(5, someQQ.length() - 1).equals(parts(10))) {
                  if (parts(4).length() <= 10 && parts(0).length() <= 10) {
                    if (parts(4).startsWith("1")) {
                      result = true
                    }
                  }
                }
              }
            }
          }
        }
        result
      }
    }.map { x =>
      {
        val parts = x.split("\t")
        //parts(0).toInt + "\t" + parts(5) + "\t" + parts(6) + "\t" + parts(7) + "\t" + parts(10) //+ "\tendMark"
        parts(0).toInt + "\t" + parts(6) + "\t" + parts(7) + "\t" + parts(10) //+ "\tendMark"
      }
    }.distinct.map { x =>
      {
        val parts = x.split("\t")
        //(parts(3) + "\t" + parts(4), 1)
        (parts(2) + "\t" + parts(3), 1)
      }
    }
    //email分析
    val email = sc.textFile("/user/wjpt/email")
    val keyEmail = email.filter { x =>
      {
        var result = false
        val parts = x.split("\t")
        if (parts.length == 27 || parts.length == 30) {
          val actionType = parts(7).toInt
          //0/发,1/收,2/web发
          if (actionType == 0 || actionType == 2) {
            if (parts(4).length() <= 10 && parts(0).length() <= 10) {
              if (parts(4).startsWith("1")) {
                result = true
              }
            }
          }
        }
        result
      }
    }.map { x =>
      {
        val parts = x.split("\t")
        //parts(0).toInt + "\t" + parts(5) + "\t" + parts(6) + "\t9\t" + parts(9).toLowerCase()
        parts(0).toInt + "\t" + parts(6) + "\t9\t" + parts(9).toLowerCase()
      }
    }.distinct.map { x =>
      {
        val parts = x.split("\t")
        //(parts(3) + "\t" + parts(4), 1)
        (parts(2) + "\t" + parts(3), 1)
      }
    }

    (keyIm union keyEmail).reduceByKey(_ + _).map { case (netid, count) => { (count, netid) } }.sortByKey(false).map { case (count, netid) => { (netid, count) } }.saveAsTextFile("/user/zhaogj/output/errnetidsamemac")
  }
}