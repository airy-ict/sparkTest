package com.qzt360.zhaogj.test.hdfs
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileUtil

object hdfsManager {
  def main(args: Array[String]) {
    val confSpark = new SparkConf().setAppName("hdfsManager")
    val sc = new SparkContext(confSpark)
    val confHadoop = new Configuration()
    val fs = FileSystem.get(confHadoop)

    val listPath = FileUtil.stat2Paths(fs.listStatus(new Path("/user/wjpt/netid/")))
    var count = 0
    for (p <- listPath) {
      //println(p)
      val netid = sc.textFile(p.toString())
      count = count + 1
      netid.filter(_.contains("qzt360")).saveAsTextFile("/user/zhaogj/output/" + count)
    }

  }
}