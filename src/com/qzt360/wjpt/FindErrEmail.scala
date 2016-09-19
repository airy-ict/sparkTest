package com.qzt360.wjpt
import org.apache.spark._
import org.apache.spark.graphx._
import scala.collection.mutable.Map
/**
 * 分析误审计的邮箱
 */
object FindErrEmail {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("FindErrEmail")
    val sc = new SparkContext(conf)
    //email分析
    val emailAll = sc.textFile("/user/wjpt/email").filter { x =>
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
    }

    val email = emailAll.map { x =>
      {
        val parts = x.split("\t")
        //email,count
        (parts(9).toLowerCase().split("@")(1), 1)
      }
    }.reduceByKey(_ + _).map {
      case (email, count) =>
        (count, email)
    }.sortByKey(false).saveAsTextFile("/user/zhaogj/output/emailCount")
  }
}