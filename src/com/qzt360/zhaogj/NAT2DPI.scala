package com.qzt360.zhaogj
import org.apache.spark.SparkContext

object NAT2DPI {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext
    val dpi = sc.textFile("/user/zhaogj/input/dpi/");
    dpi.keyBy(_.split("\',\'").length).collect
    val nat = sc.textFile("/user/zhaogj/input/nat/")
    nat.keyBy(_.split("\t").length).collect
    val dpiKeyByIpPort = dpi.keyBy(line => {
      var key = "nullkey"
      val fileds = line.split("\',\'")
      if (fileds.length == 9) {
        key = fileds(2) + ":" + fileds(3) + "," + fileds(4) + ":" + fileds(5)
      }
      key
    })

    dpiKeyByIpPort.collect
    val natKeyByIpPort = nat.keyBy(line => {
      var key = "nullkey"
      val fileds = line.split("\t")
      if (fileds.length == 10) {
        key = fileds(6) + ":" + fileds(7) + "," + fileds(4) + ":" + fileds(5)
      }
      key
    })
    natKeyByIpPort.collect
    val result = dpiKeyByIpPort.leftOuterJoin(natKeyByIpPort)

    result.saveAsTextFile("/user/zhaogj/output/resultdpi")

    dpiKeyByIpPort.join(natKeyByIpPort).collect

    //res4: Array[(String, String)] = Array((#Version 1.6:#Version 1.6,#Version 1.6), 
    //('2016-03-09 18:23:59.932908000:'2016-03-09 18:23:59.932908000,'2016-03-09 18:23:59.932908000','2016-03-09 18:23:59.932908000','111.37.5.138','60850','111.13.101.191','80','http://hm.baidu.com/h.js?ec43d3a21f91aaa24a065426f4a8a357','http','3'), 
    //('2016-03-09 18:23:12.821000000:'2016-03-09 18:23:12.821000000,'2016-03-09 18:23:12.821000000','2016-03-09 18:23:12.918000000','111.37.5.85','22172','111.13.57.18','80','http://policy.video.iqiyi.com/policy.hcdnclient.pc.xml','http','3'), ('2016-03-09 18:23:20.432000000:'2016-03-09 18:23:20.432000000,'2016-03-09 18:23:20.432000000','2016-03-09 18:23:20.433000000','111.37.6.129','43051','110.76.3.87','443','null','https','3'), ('2016-03-09 18:23:59.730043000:'2016-03-0...

    //统计dpi日志中，ip:port出现的重复概率
    dpi.count
    //166897
    val dpiIpPort = dpi.map(line => {
      var ipPort = "nullkey"
      val fileds = line.split("\',\'")
      if (fileds.length == 9) {
        ipPort = fileds(2) + ":" + fileds(3)
      }
      (ipPort, "")
    })
    dpiIpPort.collect
    //166897
    val dpiIpPortDistinct = dpiIpPort.reduceByKey(_ + _)
    10000 * dpiIpPortDistinct.count / dpi.count
    //117738
  }
}