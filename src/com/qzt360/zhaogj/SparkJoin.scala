package com.qzt360.zhaogj
import org.apache.spark.SparkContext

object SparkJoin {
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

  }
}