package com.qzt360.zhaogj
import org.apache.spark.SparkContext

object RadiusStat {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext

    val radius = sc.textFile("/user/zhaogj/input/radius/");
    //radius.keyBy(_.split("\"").length).collect
    val radiusIpAccount = radius.map(line => {
      var ipAccount = ("null", "null")
      val fileds = line.split("\"")
      if (fileds.length == 19) {
        ipAccount = (fileds(7), fileds(1))
      }
      ipAccount
    })
    radiusIpAccount.count
    //127603
    radiusIpAccount.distinct.count
    //100986
    //radiusIpAccount.collect
    val radiusIpAccountByKey = radiusIpAccount.distinct.reduceByKey(_ + "," + _)
    radiusIpAccountByKey.count
    //96795
    radiusIpAccountByKey.collect
    10000 * radiusIpAccountByKey.count / radiusIpAccount.count

    //IP distinct／total ＝ 7585
    //IP:Account distinct/total = 7914
    //find 10.214.103.215
    val tmp = radius.filter(line => line.contains("10.214.103.215"))
    tmp.collect
    //res11: Array[String] = Array(

    //<log account="17862868166" accountType="2" loginType="3" priIpAddr="10.214.103.215" pubIpAddr="" LineTimeType="2" LineTime="2016-03-09 18:08:29" LAC="" SAC="" />, 
    //<log account="17865521300" accountType="2" loginType="3" priIpAddr="10.214.103.215" pubIpAddr="" LineTimeType="1" LineTime="2016-03-09 18:08:44" LAC="" SAC="" />, 
    //<log account="17865521300" accountType="2" loginType="3" priIpAddr="10.214.103.215" pubIpAddr="" LineTimeType="2" LineTime="2016-03-09 18:14:21" LAC="" SAC="" />, 
    //<log account="17862868501" accountType="2" loginType="3" priIpAddr="10.214.103.215" pubIpAddr="" LineTimeType="1" LineTime="2016-03-09 18:14:26" LAC="" SAC="" />)

  }
}