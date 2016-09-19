package com.qzt360.wjpt
import org.apache.spark.{ SparkConf, SparkContext }
import scala.collection.mutable.ArrayBuffer

object NetIdStat {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("NetIdStat")
    val sc = new SparkContext(conf)
    //val netid = sc.textFile("/user/wjpt/netid/")
    val netid = sc.textFile("/Users/zhaogj/tmp/netidinfo_20150628")
    //75103110	407~周敏	NULL	周敏	11	440203199407246145		CHN		zhou_min_	1009	2073357835			1430409312	1430409323	2	0C:82:68:D2:77:83
    //75103110	407~周敏	NULL	周敏	11	440203199407246145		CHN		zhou_min_	1002	3168665945			1430409431	1430409431	1	0C:82:68:D2:77:83
    //76915253	08:57:00:CE:03:90(新发现)			-1						1002	1508666894			1430409365	1430409585	9	08:57:00:CE:03:90
    //76915253	9C:21:6A:3A:12:FF(新发现)			-1						1002	12633946			1430409402	1430409893	28	9C:21:6A:3A:12:FF

    val macNetidQztid = netid.map { x =>
      {
        val parts = x.split("\t")
        var output = "errLine"
        if (parts.length == 18) {
          output = parts(17) + "\t" + parts(10) + "," + parts(11) + "\t" + parts(0)
        }
        output
      }
    }
    //统计Mac漫游情况
    netid.filter { x =>
      {
        var output = false
        val parts = x.split("\t")
        //必须有18个字段
        if (parts.length == 18) {
          //mac符合基本格式
          if (parts(17).split(":").length == 6) {
            output = true
          }
        }
        output
      }
    }.map { x =>
      {
        val parts = x.split("\t")
        var output = ("null", "null")
        if (parts.length == 18) {
          output = (parts(17), parts(0))
        }
        output
      }
    }.distinct.map {
      case (mac, qztid) => {
        (mac, 1)
      }
    }.reduceByKey(_ + _).map { case (mac, count) => (count, mac) }.sortByKey(false, 1).map { case (count, mac) => (mac, count) }.saveAsTextFile("/user/zhaogj/output/macCount")

    //一个mac上产生身份个数分析
    netid.filter { x =>
      {
        var output = false
        val parts = x.split("\t")
        //必须有18个字段
        if (parts.length == 18) {
          //mac符合基本格式
          if (parts(17).split(":").length == 6) {
            //邮箱
            if (parts(10).equals("9")) {
              if (parts(11).split("@").length == 2) {
                output = true
              }
            }
            //QQ
            if (parts(10).equals("1002")) {
              if (parts(11).length() >= 5 || parts(11).length() <= 11) {
                output = true
              }
            }
          }
        }
        output
      }
    }.map { x =>
      {
        val parts = x.split("\t")
        var output = ("null", "null")
        if (parts(10).equals("9")) {
          val regEx = "\\w+([\\.-]?\\w+)*@\\w+([\\.-]?\\w+)*(\\.\\w{2,3})+".r
          val email = regEx.findFirstIn(parts(11).toLowerCase())
          output = (parts(17), parts(10) + "," + email)
        } else if (parts(10).equals("1002")) {
          output = (parts(17), parts(10) + "," + parts(11))
        }
        output
      }
    }.distinct.reduceByKey(_ + ";" + _).saveAsTextFile("/user/zhaogj/output/macNetid")

    //im分析
    val im = sc.textFile("/user/wjpt/im/im_20151001")
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
        parts(0).toInt + "\t" + parts(5) + "\t" + parts(6) + "\t" + day + "\t" + parts(7) + "\t" + parts(10) //+ "\tendMark"
      }
    }.distinct.map { x =>
      {
        val parts = x.split("\t")
        //"length:"+parts.length
        (parts(0) + "\t" + parts(1) + "\t" + parts(2) + "\t" + parts(3), parts(4) + "\t" + parts(5))
      }
    }.reduceByKey(_ + "," + _)
    keyIm.filter {
      case (key, value) => {
        var result = false
        val parts = value.split(",")
        if (parts.length <= 2) {
          result = true
        }
        result
      }
    }.saveAsTextFile("/user/zhaogj/output/tmpim")

    //email分析
    val email = sc.textFile("/user/wjpt/email/email_20151001")
    val keyEmail = email.filter { x =>
      {
        var result = false
        val parts = x.split("\t")
        if (parts.length == 27 || parts.length == 30) {
          val actionType = parts(7).toInt
          //0/发,1/收,2/web发
          if (actionType == 0 || actionType == 2) {
            result = true
          }
        }
        result
      }
    }.map { x =>
      {
        val parts = x.split("\t")
        val time = parts(4).toInt
        val day = time - (time + 60 * 60 * 8) % (60 * 60 * 24)
        parts(0).toInt + "\t" + parts(5) + "\t" + parts(6) + "\t" + day + "\t9\t" + parts(9).toLowerCase()
        //parts(0).toInt + "\t" + parts(5) + "\t" + parts(6) + "\t9\t" + parts(9).toLowerCase()
      }
    }.distinct.map { x =>
      {
        val parts = x.split("\t")
        (parts(0) + "\t" + parts(1) + "\t" + parts(2) + "\t" + parts(3), parts(4) + "\t" + parts(5))
        //(parts(0) + "\t" + parts(1) + "\t" + parts(2), parts(3) + "\t" + parts(4))
      }
    }.reduceByKey(_ + "," + _)
    import scala.collection.mutable.ArrayBuffer
    keyEmail.filter {
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
    }.saveAsTextFile("/user/zhaogj/output/tmpemail")
  }
}