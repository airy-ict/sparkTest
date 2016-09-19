package com.qzt360.zhaogj.test.mllib
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.jblas.DoubleMatrix

object MovieLens {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("MovieLens")
    val sc = new SparkContext(conf)

    val rawData = sc.textFile("/user/zhaogj/input/u.data")
    rawData.first()

    val rawRatings = rawData.map(_.split("\t").take(3))
    rawRatings.first()

    val ratings = rawRatings.map {
      case Array(user, movie, rating) =>
        Rating(user.toInt, movie.toInt, rating.toDouble)
    }
    ratings.first()

    val model = ALS.train(ratings, 50, 10, 0.01)
    model.userFeatures.count()
    model.productFeatures.count()

    val predictedRating = model.predict(789, 123)

    val userId = 789
    val K = 10
    val topKRecs = model.recommendProducts(userId, K)

    val movies = sc.textFile("/user/zhaogj/input/u.item")
    val titles = movies.map(line => line.split("\\|").take(2)).map(array => (array(0).toInt, array(1))).collectAsMap()
    titles(123)

    val moviesForUser = ratings.keyBy(_.user).lookup(789)
    println(moviesForUser.size)

    moviesForUser.sortBy(-_.rating).take(10).map(rating => (titles(rating.product), rating.rating)).foreach(println)

    topKRecs.map(rating => (titles(rating.product), rating.rating)).foreach(println)

    val aMatrix = new DoubleMatrix(Array(1.0, 2.0, 3.0))

    def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
      vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
    }

    val itemId = 567
    val itemFactor = model.productFeatures.lookup(itemId).head
    val itemVector = new DoubleMatrix(itemFactor)
    cosineSimilarity(itemVector, itemVector)

    val sims = model.productFeatures.map {
      case (id, factor) =>
        val factorVector = new DoubleMatrix(factor)
        val sim = cosineSimilarity(factorVector, itemVector)
        (id, sim)
    }

    val sortedSims = sims.top(K)(Ordering.by[(Int, Double), Double] {
      case (id, similarity) => similarity
    })
    println(sortedSims.take(10).mkString("\n"))
    println(titles(itemId))
  }
}