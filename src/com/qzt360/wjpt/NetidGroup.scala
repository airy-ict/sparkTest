package com.qzt360.wjpt

import org.apache.spark._
import org.apache.spark.graphx._
import scala.collection.mutable.Map

object NetidGroup {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("NetidGroupZhaogj")
    val sc = new SparkContext(conf)
    //im分析
    //滤掉明显不合格的数据
    val im = sc.textFile("/user/wjpt/im").filter { x =>
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
    }
    //找出只出现一次的数据
    val imOnce = im.map { x =>
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
        //netIdType netId, count
        (parts(4) + "\t" + parts(5), 1)
      }
    }.reduceByKey(_ + _).filter {
      case (netid, count) => {
        var output = false
        if (count == 1) {
          output = true
        }
        output
      }
    }
    //四处乱飞的QQ
    val imMany = im.map { x =>
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
        if (count >= 20) {
          output = true
        }
        output
      }
    }
    val errIm = imOnce union imMany
    errIm.saveAsTextFile("/user/zhaogj/output/errIm")
    
    val errImMap = errIm.collectAsMap

    val keyIm = im.filter { x =>
      {
        var result = false
        val parts = x.split("\t")
        if ("None".equals("" + errImMap.get(parts(7) + "\t" + parts(10)))) {
          result = true
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
        //同一个人最多也就有三个QQ
        if (parts.length <= 3) {
          result = true
        }
        result
      }
    }
    keyIm.saveAsTextFile("/user/zhaogj/output/imRelation")

    //email分析
    val email = sc.textFile("/user/wjpt/email").filter { x =>
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
    //找出只出现一次的邮箱，认为是误审计
    val emailOnce = email.map { x =>
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
        //netIdType netId, count
        (parts(4) + "\t" + parts(5), 1)
      }
    }.reduceByKey(_ + _).filter {
      case (netid, count) => {
        var output = false
        if (count == 1) {
          output = true
        }
        output
      }
    }
    //四处乱飞的email
    val emailMany = email.map { x =>
      {
        val parts = x.split("\t")
        val time = parts(4).toInt
        val day = time - (time + 60 * 60 * 8) % (60 * 60 * 24)
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
        if (count >= 20) {
          output = true
        }
        output
      }
    }

    val errEmail = emailOnce union emailMany
    errEmail.saveAsTextFile("/user/zhaogj/output/errEmail")
    val errEmailMap = errEmail.collectAsMap

    val keyEmail = email.filter { x =>
      {
        var result = false
        val parts = x.split("\t")
        if ("None".equals("" + errEmailMap.get("9\t" + parts(9).toLowerCase()))) {
          result = true
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
        var emailFix = Map[String, Int]()
        for (tmp <- parts) {
          val emails = tmp.split("@")
          if ("None".equals("" + emailFix.get(emails(1)))) {
            emailFix(emails(1)) = 1
          } else {
            result = false
          }
        }
        result
      }
    }
    keyEmail.saveAsTextFile("/user/zhaogj/output/emailRelation")

    val netids = (keyIm union keyEmail).reduceByKey(_ + "," + _).map { case (key, value) => value }.filter { x =>
      {
        var output = false
        if (x.split(",").length >= 2) {
          output = true
        }
        output
      }
    }
    netids.saveAsTextFile("/user/zhaogj/output/netIdRelation")

    //计算最大连通子图，看看结果是什么鸟样
    //给每个身份一个唯一编号(Long)
    //每个partition中编号各自编写，要注意
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
    sc.stop
  }
}