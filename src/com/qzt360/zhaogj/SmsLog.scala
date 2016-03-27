package com.qzt360.zhaogj
import org.apache.spark.SparkContext

object SmsLog {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext
    //Read rating from HDFS file
    //UserID::MovieID::Rating::Timestamp
    val ratingFile = sc.textFile(args(0))
    //extract (movieid,rating)
    val rating = ratingFile.map(line => {
      val fileds = line.split("::")
      (fileds(1).toInt, fileds(2).toDouble)
    })
    val movieScores = rating.groupByKey().map(data => {
      val avg = data._2.sum / data._2.size
      (data._1, avg)
    })
    //Read movie from HDFS file
    //MovieID::Title::Genres
    val moviesFile = sc.textFile(args(1))
    val movieskey = moviesFile.map(line => {
      val fileds = line.split("::")
      (fileds(0).toInt, fileds(1))
    }).keyBy(tup => tup._1)
    //by join, we get <movie, averageRating, movieName>
    val result = movieScores.keyBy(tup => tup._1).join(movieskey).filter(f => f._2._1._2 > 4.0).map(f => (f._1, f._2._1._2, f._2._2._2))
    result.saveAsTextFile(args(2))

    //HotelLog SMSLog
    //| seq_no | service_id | customer_id | telephone | content | create_time | send_time | send_result |
    val smslog = sc.textFile("/user/zhaogj/input/tbl_sms_20160307.sql")
    //找出时间在1453915000后的，20160127，排除博弈的短信
    val smslog0127 = smslog.filter(line => {
      var bResult = false
      val fileds = line.split("\t")
      if (fileds(5).toInt > 1453915000 && fileds(2).toString().length() > 5 && !fileds(2).toString().startsWith("8")) {
        bResult = true
      }
      bResult
    })
    val smslogDay = smslog0127.map(line => {
      val fileds = line.split("\t")
      val create_time = fileds(5).toInt
      val create_day = ((create_time + 60 * 60 * 8) / (60 * 60 * 24)) * (60 * 60 * 24) - (60 * 60 * 8)
      fileds(0) + "\t" + fileds(1) + "\t" + fileds(2) + "\t" + fileds(3) + "\t" + fileds(4) + "\t" + create_day + "\t" + fileds(6) + "\t" + fileds(7) + "\t" + create_time
    })
    smslog0127.saveAsTextFile("/user/zhaogj/output201601271/")
    smslogDay.saveAsTextFile("/user/zhaogj/output20160127day/")
    val smslogDay3 = smslog0127.map(line => {
      val fileds = line.split("\t")
      val create_time = fileds(5).toInt
      val create_day = ((create_time + 60 * 60 * 8) / (60 * 60 * 24 * 3)) * (60 * 60 * 24 * 3) - (60 * 60 * 8)
      fileds(0) + "\t" + fileds(1) + "\t" + fileds(2) + "\t" + fileds(3) + "\t" + fileds(4) + "\t" + create_day + "\t" + fileds(6) + "\t" + fileds(7) + "\t" + create_time
    })
    smslogDay3.saveAsTextFile("/user/zhaogj/output20160127day3/")
    
    val smslogDay5 = smslog0127.map(line => {
      val fileds = line.split("\t")
      val create_time = fileds(5).toInt
      val create_day = ((create_time + 60 * 60 * 8) / (60 * 60 * 24 * 5)) * (60 * 60 * 24 * 5) - (60 * 60 * 8)
      fileds(0) + "\t" + fileds(1) + "\t" + fileds(2) + "\t" + fileds(3) + "\t" + fileds(4) + "\t" + create_day + "\t" + fileds(6) + "\t" + fileds(7) + "\t" + create_time
    })
    smslogDay5.saveAsTextFile("/user/zhaogj/output20160127day5/")
  }
}