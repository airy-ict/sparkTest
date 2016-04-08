package com.qzt360.zhaogj.test.examples
import org.apache.spark.{ SparkConf, SparkContext }
/**
 * 读取http审计文件，计算url访问次数排行
 * 逐行读取文件内容，获取合法的对应字段，整理对应字段，计算出现次数，排行数据输出
 */
object FindUrlRank {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("FindUrlRank")
    val sc = new SparkContext(conf)
    /**
     * httpData eg:
     * '2016-03-09 18:23:16.645000000','2016-03-09 18:23:16.686000000','111.34.48.160','57320','111.13.100.92','80','http://www.baidu.com/?s=20160309182644','http','3'
     * '2016-03-09 18:23:23.376000000','2016-03-09 18:23:23.577000000','111.37.6.41','19672','183.203.30.20','80','null','null','3'
     * '2016-03-09 18:22:56.243000000','2016-03-09 18:23:20.219000000','111.37.5.2','45915','183.136.217.16','80','http://pic.fastapi.net/sdk/js/ai.m.js','http','3'
     */
    //文件变成一个RDD
    val fileRDD = sc.textFile("/user/zhaogj/input/httpData")
    val urlRDD = fileRDD.map { x =>
      {
        var url = "null"
        val parts = x.split("'");
        if (parts.length == 18) {
          if (parts(13).startsWith("http://") || parts(13).startsWith("https://")) {
            var urlTmp = parts(13).toLowerCase().replaceAll("http://", "").replaceAll("https://", "")
            if (urlTmp.indexOf("/") > 0) {
              url = urlTmp.substring(0, urlTmp.indexOf("/"))
            }
          }
        }
        url
      }
    }
    urlRDD.collect

    val resultOld = urlRDD.filter(!_.equals("null")).map { x => (x, 1) }.reduceByKey(_ + _).map {
      case (key, value) => (value, key);
    }.sortByKey(false, 1).map {
      case (key, value) => (value, key);
    }
    resultOld.collect

    val result = urlRDD.filter(!_.equals("null")).map { x => (x, 1) }.combineByKey(
      (v: Int) => v,
      (c: Int, v: Int) => c + v,
      (c1: Int, c2: Int) => c1 + c2).map {
        case (key, value) => (value, key);
      }.sortByKey(false, 1).map {
        case (key, value) => (value, key);
      }
    result.collect
    result.saveAsTextFile("/user/zhaogj/output/httpData")
  }
}