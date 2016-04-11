package com.qzt360.zhaogj.test.examples
import org.apache.spark.{ SparkConf, SparkContext }

/**
 * Radius日志内容的分析
 */
object RadiusStat {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("FindUrlRank")
    val sc = new SparkContext(conf)
    //计算每天出现手机号的个数

    //val radiusData = sc.textFile("/Users/zhaogj/work/datasets/wlansd/NUU-530000018553-20160309183415.xml.ok")
    //val radiusData = sc.textFile("/Users/zhaogj/work/datasets/wlansd/LUU-530000022692-20160331075011.xml.ok")
    val radiusData = sc.textFile("/user/wlan/radius/history/*.ok").coalesce(848, false)
    //NUU
    //<?xml version="1.0" encoding="UTF-8"?>
    //<info id="530000018553" type="nat_login_info" resultnum="127600">
    //<log account="15092827257" accountType="2" loginType="3" priIpAddr="10.230.134.150" pubIpAddr="" LineTimeType="2" LineTime="2016-03-09 18:04:16" LAC="" SAC="" />
    //<log account="15064895640" accountType="2" loginType="3" priIpAddr="10.252.249.165" pubIpAddr="" LineTimeType="2" LineTime="2016-03-09 18:04:16" LAC="" SAC="" />
    //<log account="14706475257" accountType="2" loginType="3" priIpAddr="10.196.109.168" pubIpAddr="" LineTimeType="2" LineTime="2016-03-09 18:04:16" LAC="" SAC="" />

    //LUU
    //<?xml version="1.0" encoding="UTF-8"?>
    //<info id="530000022692" type="cdr_login_info" resultnum="43503">
    //<log account="17866671303" accountType="2" loginType="3" ipAddr="223.78.222.13" LineTimeType="2" LineTime="2016-03-31 07:20:11" LAC="" SAC="" />
    //<log account="13806347628" accountType="2" loginType="3" ipAddr="218.201.179.148" LineTimeType="2" LineTime="2016-03-31 07:20:11" LAC="" SAC="" />
    //<log account="14753384067" accountType="2" loginType="3" ipAddr="111.15.9.204" LineTimeType="2" LineTime="2016-03-31 07:20:11" LAC="" SAC="" />

    val dayTel = radiusData.filter { x =>
      {
        var result = false;
        val parts = x.split("\"")
        if (parts.length == 19 || parts.length == 17) {
          result = true
        }
        result
      }
    }.map { x =>
      {
        val parts = x.split("\"")
        if (parts.length == 19) {
          parts(1) + "\t" + parts(13).split(" ")(0)
        } else {
          parts(1) + "\t" + parts(11).split(" ")(0)
        }
      }
    }.distinct()
    dayTel.map { x =>
      {
        val parts = x.split("\t")
        (parts(1), 1)
      }
    }.reduceByKey(_ + _).saveAsTextFile("/Users/zhaogj/tmp/output/dayCount")

    radiusData.filter { x =>
      {
        var result = false;
        val parts = x.split("\"")
        if (parts.length == 19 || parts.length == 17) {
          result = true
        }
        result
      }
    }.map { x =>
      {
        val parts = x.split("\"")
        if (parts.length == 19) {
          parts(1) + "\t" + parts(13).split(" ")(0)
        } else {
          parts(1) + "\t" + parts(11).split(" ")(0)
        }
      }
    }.distinct().map { x =>
      {
        val parts = x.split("\t")
        (parts(1), 1)
      }
    }.reduceByKey(_ + _).saveAsTextFile("/user/zhaogj/output/dayCount")

    //计算每天活跃的IP
    //val radiusData = sc.textFile("/user/wlan/radius/history/NUU*.ok").coalesce(400, false)
    //val radiusData = sc.textFile("/user/wlan/radius/history/LUU*.ok").coalesce(800, false)
    radiusData.filter { x =>
      {
        var result = false;
        val parts = x.split("\"")
        if (parts.length == 19 || parts.length == 17) {
          result = true
        }
        result
      }
    }.map { x =>
      {
        val parts = x.split("\"")
        if (parts.length == 19) {
          parts(7) + "\t" + parts(13).split(" ")(0)
        } else {
          parts(7) + "\t" + parts(11).split(" ")(0)
        }
      }
    }.distinct().map { x =>
      {
        val parts = x.split("\t")
        (parts(1), 1)
      }
    }.reduceByKey(_ + _).saveAsTextFile("/user/zhaogj/output/dayLuuIpCount")
  }
}