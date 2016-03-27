package com.qzt360.zhaogj
import org.apache.spark.SparkContext

object SparkJoinTest {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext
    val ratingFile = sc.textFile("/user/zhaogj/input/ml-1m/ratings.dat")
    ratingFile.saveAsTextFile("/user/zhaogj/output/ratingFile")
    //1::1193::5::978300760
    //1::661::3::978302109
    //1::914::3::978301968

    //extract (movieid,rating)
    val rating = ratingFile.map(line => {
      val fileds = line.split("::")
      (fileds(1).toInt, fileds(2).toDouble)
    })
    rating.saveAsTextFile("/user/zhaogj/output/rating")
    //(1193,5.0)
    //(661,3.0)
    //(914,3.0)

    val movieScores = rating.groupByKey().map(data => {
      val avg = data._2.sum / data._2.size
      (data._1, avg)
    })
    movieScores.saveAsTextFile("/user/zhaogj/output/movieScores")
    //(3586,3.392857142857143)
    //(1084,4.096209912536443)
    //(3456,3.9384615384615387)

    //Read movie from HDFS file
    //MovieID::Title::Genres
    val moviesFile = sc.textFile("/user/zhaogj/input/ml-1m/movies.dat")
    moviesFile.saveAsTextFile("/user/zhaogj/output/moviesFile")
    //1::Toy Story (1995)::Animation|Children's|Comedy
    //2::Jumanji (1995)::Adventure|Children's|Fantasy
    //3::Grumpier Old Men (1995)::Comedy|Romance

    val movieskey = moviesFile.map(line => {
      val fileds = line.split("::")
      (fileds(0).toInt, fileds(1))
    }).keyBy(tup => tup._1)
    movieskey.saveAsTextFile("/user/zhaogj/output/movieskey")
    //(1,(1,Toy Story (1995)))
    //(2,(2,Jumanji (1995)))
    //(3,(3,Grumpier Old Men (1995)))

    //by join, we get <movie, averageRating, movieName>
    movieScores.keyBy(tup => tup._1).join(movieskey).saveAsTextFile("/user/zhaogj/output/resultjoin")
    //(1084,((1084,4.096209912536443),(1084,Bonnie and Clyde (1967))))
    //(3456,((3456,3.9384615384615387),(3456,Color of Paradise, The (Rang-e Khoda) (1999))))
    //(1410,((1410,2.8666666666666667),(1410,Evening Star, The (1996))))
    
    val result = movieScores.keyBy(tup => tup._1).join(movieskey).filter(f => f._2._1._2 > 4.0).map(f => (f._1, f._2._1._2, f._2._2._2))
    result.saveAsTextFile("/user/zhaogj/output/result")
    //(1084,4.096209912536443,Bonnie and Clyde (1967))
    //(912,4.412822049131217,Casablanca (1942))
    //(1148,4.507936507936508,Wrong Trousers, The (1993))
  }
}