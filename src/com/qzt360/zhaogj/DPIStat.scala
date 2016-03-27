package com.qzt360.zhaogj
import org.apache.spark.SparkContext

object DPIStat {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext
    //统计dpi日志中，ip:port出现的重复概率
    //-rw-r--r--   3 hdfs supergroup   10934976 2016-03-10 06:14 /user/wlan/dpi/nat/current/AHTTPP5352D20160309062204E172.txt.ok 6428
    //-rw-r--r--   3 hdfs supergroup   10985139 2016-03-10 06:15 /user/wlan/dpi/nat/current/AHTTPP5352D20160309062405E176.txt.ok 6168
    //-rw-r--r--   3 hdfs supergroup   11150196 2016-03-10 06:15 /user/wlan/dpi/nat/current/AHTTPP5352D20160309062605E180.txt.ok 6533
    //-rw-r--r--   3 hdfs supergroup   12235809 2016-03-10 06:16 /user/wlan/dpi/nat/current/AHTTPP5352D20160309063806E204.txt 6622
    //-rw-r--r--   3 hdfs supergroup   12325624 2016-03-10 06:16 /user/wlan/dpi/nat/current/AHTTPP5352D20160309063906E206.txt.ok 6593
    //-rw-r--r--   3 hdfs supergroup   13254346 2016-03-10 06:17 /user/wlan/dpi/nat/current/AHTTPP5352D20160309064307E214.txt 6858
    //-rw-r--r--   3 hdfs supergroup   13856043 2016-03-10 06:17 /user/wlan/dpi/nat/current/AHTTPP5352D20160309064607E220.txt.ok 6530
    ///user/wlan/dpi/nat/current/*20160309* 3723

    //-rw-r--r--   3 hdfs supergroup   22258536 2016-03-10 07:32 /user/wlan/dpi/nat/history/20160310071500/AHTTPP5303D20160310072600E000.txt.ok 126
    //-rw-r--r--   3 hdfs supergroup   22427698 2016-03-10 07:36 /user/wlan/dpi/nat/history/20160310071500/AHTTPP5303D20160310073000E000.txt.ok 129
    //-rw-r--r--   3 hdfs supergroup   27486906 2016-03-10 07:23 /user/wlan/dpi/nat/history/20160310071500/AHTTPP5304D20160310071600E000.txt.ok
    //-rw-r--r--   3 hdfs supergroup   24839281 2016-03-10 07:26 /user/wlan/dpi/nat/history/20160310071500/AHTTPP5304D20160310072000E000.txt.ok
    //-rw-r--r--   3 hdfs supergroup   23134502 2016-03-10 07:26 /user/wlan/dpi/nat/history/20160310071500/AHTTPP5304D20160310072200E000.txt.ok
    //-rw-r--r--   3 hdfs supergroup   26210391 2016-03-10 07:32 /user/wlan/dpi/nat/history/20160310071500/AHTTPP5304D20160310072400E000.txt.ok
    val dpi = sc.textFile("/user/zhaogj/input/dpi/AHTTPP5313D20160309182400E000.txt.ok")
    //val dpi = sc.textFile("/user/wlan/dpi/nat/history/20160310071500/AHTTPP5303D20160310073000E000.txt.ok")
    val keyByTmp = dpi.keyBy { x =>
      var natIp = "nullkey"
      val fileds = x.split("\',\'")
      if (fileds.length == 9) {
        natIp = fileds(2)
      }
      natIp
    }
    val reduceByKeyTmp = keyByTmp.reduceByKey { (key, value) =>
      var natIp = "nullkey"
      val fileds = value.split("\',\'")
      if (fileds.length == 9) {
        natIp = fileds(2)
      }
      value + value
    }

    val mapTmp = dpi.map { x =>
      var natIp = "nullkey"
      var portTime = "nullportTime"
      val fileds = x.split("\',\'")
      if (fileds.length == 9) {
        //natIp = fileds(2)
        natIp = fileds(2) + ":" + fileds(3)
        //portTime = fileds(3)+","+fileds(1)
        portTime = fileds(1)
      }
      (natIp, portTime)
    }.reduceByKey(_ + "-" + _)
    mapTmp.saveAsTextFile("/user/zhaogj/output/34")

    dpi.filter(_.contains("111.37.8.25")).filter(_.contains("60664")).saveAsTextFile("/user/zhaogj/output/35")
    
    val dpiIpPort = dpi.map(line => {
      var ipPort = "nullkey"
      val fileds = line.split("\',\'")
      if (fileds.length == 9) {
        ipPort = fileds(2) + ":" + fileds(3) + "," + fileds(4) + ":" + fileds(5)
      }
      (ipPort, "")
    }).reduceByKey(_ + _)
    10000 * dpiIpPort.count / dpi.count

    val dpiIpPorts = dpi.map(line => {
      var ipPort = ("nullIp", "nullPort")
      val fileds = line.split("\',\'")
      if (fileds.length == 9) {
        ipPort = (fileds(2), fileds(3))
      }
      ipPort
    }).distinct.reduceByKey(_ + "," + _)

    //统计ip:port,dstIp:dstPort的重复度
    val natIpPortdstIpPort = dpi.map(line => {
      var ipPort = "nullkey"
      val fileds = line.split("\',\'")
      if (fileds.length == 9) {
        ipPort = fileds(2) + ":" + fileds(3) + "," + fileds(4) + ":" + fileds(5)
      }
      (ipPort, 1)
    })
    (10000 * natIpPortdstIpPort.distinct.count) / natIpPortdstIpPort.count
    natIpPortdstIpPort.reduceByKey(_ + _).filter(x => x._2 > 10) collect
    //111.37.5.135:63599,111.13.100.195:80
    val tmp1 = dpi.filter(_.contains("111.37.5.135"))
    tmp1.saveAsTextFile("/user/zhaogj/output/tmp1")
    val tmp2 = tmp1.filter(_.contains("63599")).filter(_.contains("111.13.100.195")).filter(_.contains("80"))
    tmp2.saveAsTextFile("/user/zhaogj/output/tmp2")
  }
}