package com.qzt360.wjpt

import org.apache.spark._
import org.apache.spark.graphx._
import scala.collection.mutable.ArrayBuffer

object NetidGroup {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("NetidGroup")
    val sc = new SparkContext(conf)
    //im分析
    val im = sc.textFile("/user/wjpt/im")
    var errIms = new ArrayBuffer[String]()
    im.filter { x =>
      {
        var result = false
        val parts = x.split("\t")
        if (parts.length == 19 || parts.length == 22) {
          if (parts(7).equals("1002")) {
            if (parts(10).length() >= 5 && parts(10).length() <= 11) {
              val regEx = "[0-9]+".r
              val someQQ = regEx.findFirstIn(parts(10)).toString
              if (!someQQ.equals("None")) {
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
        //customerId mac netIdType netId
        parts(0).toInt + "\t" + parts(6) + "\t" + parts(7) + "\t" + parts(10)
      }
    }.distinct.map { x =>
      {
        val parts = x.split("\t")
        //netIdType netId, count
        (parts(2) + "\t" + parts(3), 1)
      }
    }.reduceByKey(_ + _).filter {
      case (netid, count) => {
        var output = false
        if (count > 5) {
          output = true
        }
        output
      }
    }.foreach {
      case (netid, count) =>
        {
          errIms += netid
        }
    }

    val keyIm = im.filter { x =>
      {
        var result = false
        val parts = x.split("\t")
        if (parts.length == 19 || parts.length == 22) {
          if (parts(7).equals("1002")) {
            if (parts(10).length() >= 5 && parts(10).length() <= 11) {
              val regEx = "[0-9]+".r
              val someQQ = regEx.findFirstIn(parts(10)).toString
              if (!someQQ.equals("None")) {
                if (someQQ.substring(5, someQQ.length() - 1).equals(parts(10))) {
                  if (parts(4).length() <= 10 && parts(0).length() <= 10) {
                    if (parts(4).startsWith("1")) {
                      if (!errIms.contains(parts(7) + "\t" + parts(10))) {
                        result = true
                      }
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
        val time = parts(4).toInt
        val day = time - (time + 60 * 60 * 8) % (60 * 60 * 24)
        //customerId ip mac day netIdType netId
        parts(0).toInt + "\t" + parts(5) + "\t" + parts(6) + "\t" + day + "\t" + parts(7) + "\t" + parts(10)
      }
    }.distinct.map { x =>
      {
        val parts = x.split("\t")
        //customerId ip mac day, netIdType netId
        (parts(0) + "\t" + parts(1) + "\t" + parts(2) + "\t" + parts(3), parts(4) + "\t" + parts(5))
      }
    }.reduceByKey(_ + "," + _).filter {
      case (key, value) => {
        var result = false
        val parts = value.split(",")
        if (parts.length <= 2) {
          result = true
        }
        result
      }
    }
    //keyIm.saveAsTextFile("/user/zhaogj/output/tmpim")

    //email分析
    val email = sc.textFile("/user/wjpt/email")
    var errEmails = new ArrayBuffer[String]()
    email.filter { x =>
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
        //customerId mac netIdType netId
        parts(0).toInt + "\t" + parts(6) + "\t9\t" + parts(9).toLowerCase()
      }
    }.distinct.map { x =>
      {
        val parts = x.split("\t")
        //netIdType netId, count
        (parts(2) + "\t" + parts(3), 1)
      }
    }.reduceByKey(_ + _).filter {
      case (netid, count) => {
        var output = false
        if (count > 5) {
          output = true
        }
        output
      }
    }.foreach {
      case (netid, count) =>
        {
          errEmails += netid
        }
    }

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
                if (!errEmails.contains("9\t" + parts(10))) {
                  result = true
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
        val time = parts(4).toInt
        val day = time - (time + 60 * 60 * 8) % (60 * 60 * 24)
        //customerId ip mac day netIdType netId
        parts(0).toInt + "\t" + parts(5) + "\t" + parts(6) + "\t" + day + "\t9\t" + parts(9).toLowerCase()
      }
    }.distinct.map { x =>
      {
        val parts = x.split("\t")
        //customerId ip mac day, netIdType netId
        (parts(0) + "\t" + parts(1) + "\t" + parts(2) + "\t" + parts(3), parts(4) + "\t" + parts(5))
      }
    }.reduceByKey(_ + "," + _).filter {
      case (key, value) => {
        var result = true
        val parts = value.split(",")
        var emailFix = new ArrayBuffer[String]()
        for (tmp <- parts) {
          val emails = tmp.split("@")
          if (!emailFix.contains(emails(1))) {
            emailFix += emails(1)
          } else {
            result = false
          }
        }
        result
      }
    }
    //keyEmail.saveAsTextFile("/user/zhaogj/output/tmpemail")
    val netids = (keyIm union keyEmail).reduceByKey(_ + "," + _).map { case (key, value) => value }.filter { x =>
      {
        var output = false
        if (x.split(",").length >= 2) {
          output = true
        }
        output
      }
    }
    //netids.saveAsTextFile("/user/zhaogj/output/netids5")

    //计算最大连通子图，看看结果是什么鸟样
    //给每个身份一个唯一编号(Long)
    var idNum = 0L
    val netidNumMap = netids.coalesce(1).map(x => x.split(",")).flatMap { x =>
      {
        for (e <- x) yield e
      }
    }.distinct.map { x =>
      {
        idNum = idNum + 1
        (x, idNum)
      }
    }.collectAsMap
    netids.coalesce(1).map(x => x.split(",")).flatMap { x =>
      {
        for (e <- x) yield e
      }
    }.distinct.map { x =>
      {
        idNum = idNum + 1
        x + "," + idNum
      }
    }.saveAsTextFile("/user/zhaogj/output/netidNum")
    netids.coalesce(1).map(x => x.split(",")).flatMap { x =>
      {
        for (i <- 1 until x.length - 1) yield (netidNumMap(x(0)) + "\t" + netidNumMap(x(i)))
      }
    }.saveAsTextFile("/user/zhaogj/output/relations")

    val graph = GraphLoader.edgeListFile(sc, "/user/zhaogj/output/relations")
    val cc = graph.connectedComponents().vertices
    val users = sc.textFile("/user/zhaogj/output/netidNum").map { line =>
      val fields = line.split(",")
      (fields(1).toLong, fields(0))
    }
    val ccByUsername = users.join(cc).map {
      case (id, (username, cc)) => (username, cc)
    }.map { case (username, userid) => (userid, username) }.reduceByKey(_ + "," + _).saveAsTextFile("/user/zhaogj/output/result")
  }
}