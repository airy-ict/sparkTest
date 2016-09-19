package com.qzt360.zhaogj.test.mllib
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.impurity.Entropy

object StumbleUpon {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MovieLens")
    val sc = new SparkContext(conf)

    val rawData = sc.textFile("/Users/zhaogj/work/datasets/stumbleupon/train_noheader.tsv")
    val records = rawData.map(line => line.split("\t"))
    records.first()

    val data = records.map { r =>
      val trimmed = r.map(_.replaceAll("\"", ""))
      val label = trimmed(r.size - 1).toInt
      val features = trimmed.slice(4, r.size - 1).map(d => if (d == "?") 0.0 else d.toDouble).map(d => if (d < 0) 0.0 else d)
      LabeledPoint(label, Vectors.dense(features))
    }
    data.cache
    val numData = data.count()

    val numIterations = 10
    val maxTreeDepth = 5

    val lrModel = LogisticRegressionWithSGD.train(data, numIterations)

    val svmModel = SVMWithSGD.train(data, numIterations)

    //val nbModel = NaiveBayes.train(nbData)

    val dtModel = DecisionTree.train(data, Algo.Classification, Entropy, maxTreeDepth)

    val dataPoint = data.first

    val prediction = lrModel.predict(dataPoint.features)

    val trueLabel = dataPoint.label
    val predictions = lrModel.predict(data.map(lp => lp.features))
    predictions.take(5)
    data.take(5)
    val lrTotalCorrect = data.map { point =>
      if (lrModel.predict(point.features) == point.label) 1 else 0
    }.sum
    val lrAccuracy = lrTotalCorrect / data.count

    val svmTotalCorrect = data.map { point =>
      if (svmModel.predict(point.features) == point.label) 1 else 0
    }.sum

    val svmAccuracy = svmTotalCorrect / data.count

    //val nbTotalCorrect = nbData.map { point =>
    // if (nbModel.predict(point.features) == point.label) 1 else 0
    // }.sum

    //val nbAccuracy = nbTotalCorrect / data.count

    val dtTotalCorrect = data.map { point =>
      val score = dtModel.predict(point.features)
      val predicted = if (score > 0.5) 1 else 0
      if (predicted == point.label) 1 else 0
    }.sum

    val dtAccuracy = dtTotalCorrect / data.count
  }
}