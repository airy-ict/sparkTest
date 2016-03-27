package com.qzt360.zhaogj
import org.apache.spark._

object Radius2DPI {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("SparkHBaseSpeedTest")
    val sc = new SparkContext(sparkConf)
    val dpi = sc.textFile("/user/zhaogj/input/dpi/");
    val radius = sc.textFile("/user/zhaogj/input/radius/")

    val radiusKeyByIp = radius.keyBy(line => {
      var key = "errkey"
      val fileds = line.split("\"")
      if (fileds.length == 19) {
        if (fileds(11).toInt == 1) {
          key = fileds(7)
        }
      }
      key
    })
    val dpiKeyByIp = dpi.keyBy(line => {
      var key = "nullkey"
      val fileds = line.split("\',\'")
      if (fileds.length == 9) {
        key = fileds(2)
      }
      key
    })
    //val result = dpiKeyByIp.leftOuterJoin(radiusKeyByIp)
    val result = dpiKeyByIp.join(radiusKeyByIp)
    //res0: Array[(String, (String, String))] = Array(
    //(192.168.39.35,('2016-03-11 08:30:00.821000000','2016-03-11 08:31:00.000000000','192.168.39.35','20000','125.88.108.111','80','http://gd.wjpt.com','http','4',<log account="13391879355" accountType="2" loginType="3" priIpAddr="192.168.39.35" pubIpAddr="" LineTimeType="1" LineTime="2016-03-11 08:00:00" LAC="" SAC="" />)), 
    //(192.168.39.35,('2016-03-11 08:30:00.821000000','2016-03-11 08:31:00.000000000','192.168.39.35','20000','125.88.108.111','80','http://gd.wjpt.com','http','4',<log account="15010281799" accountType="2" loginType="3" priIpAddr="192.168.39.35" pubIpAddr="" LineTimeType="1" LineTime="2016-03-11 09:01:00" LAC="" SAC="" />)), 
    //(192.168.39.35,('2016-03-11 09:30:00.821000000','2016-03-11 09:31:00.000000000','192.168.39.35','20000','...

    result.filter(f => {
      f._2._1.split("\',\'")(1) < (f._2._2.split("\"")(13) + ".999999999")
    }).collect
    //res1: Array[(String, (String, String))] = Array(
    //(192.168.39.35,('2016-03-11 08:30:00.821000000','2016-03-11 08:31:00.000000000','192.168.39.35','20000','125.88.108.111','80','http://gd.wjpt.com','http','4',<log account="15010281799" accountType="2" loginType="3" priIpAddr="192.168.39.35" pubIpAddr="" LineTimeType="1" LineTime="2016-03-11 09:01:00" LAC="" SAC="" />)))

    
  }
}