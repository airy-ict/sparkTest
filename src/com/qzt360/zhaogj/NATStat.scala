package com.qzt360.zhaogj
import org.apache.spark.SparkContext

object NATStat {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext
    //1	close	10.213.214.7	38768	60.28.208.179	80	111.37.3.103	41009	2016-03-09 18:24:15.0	17
    //1	close	10.214.102.119	56209	111.13.142.4	80	111.37.3.50	41176	2016-03-09 18:24:15.0	79
    //1	close	10.211.227.46	57088	211.137.185.106	8001	111.37.3.55	16256	2016-03-09 18:24:15.0	18
    //1	close	10.213.212.26	34563	183.61.95.6	17788	111.37.3.96	34360	2016-03-09 18:24:15.0	125
    //val nat = sc.textFile("/user/zhaogj/input/nat/")
    val nat = sc.textFile("/user/zhaogj/nat/")
    val natIpPortDstIpPort = nat.map(line => {
      var ipPort = "nullkey"
      val fileds = line.split("\t")
      if (fileds.length == 10) {
        //ipPort = fileds(6) + ":" + fileds(7) + "," + fileds(4) + ":" + fileds(5) + "," + fileds(2) + ":" + fileds(3)
        ipPort = fileds(4) + ":" + fileds(6)
      }
      (ipPort, 1)
    })
    nat.count
    //9600000
    nat.distinct.count
    //9465747
    natIpPortDstIpPort.count
    //9600000
    natIpPortDstIpPort.distinct.count
    //natIp:140
    //natIpPort:4861587 50.64%
    //natIpPortDstIpPort:8090827 84.27%
    //natIpPortDstIpPortLanIpPort:8410700 87.61%
    //LanIpPortDstIpPort:8296349 86.42%
    //LanIpPort:76.43%
    //LanIp:0.74%
    //LanIpNatIp:0.74%
    //natIpDstIp:12.68%
    (10000 * natIpPortDstIpPort.distinct.count) / natIpPortDstIpPort.count
    natIpPortDstIpPort.reduceByKey(_ + _).filter(x => x._2 > 1) collect
  }
}