package com.qzt360.zhaogj
import org.apache.spark.{ SparkConf, SparkContext }

/**
 * 内网集群上测试查询的速度<br>
 * spark-submit --num-executors 64 --class com.qzt360.zhaogj.QuerySpeedTest sparkTest-0.0.1-SNAPSHOT.jar /user/zhaogj/input/30G /user/zhaogj/output <br>
 * spark-submit --num-executors 2 --class com.qzt360.zhaogj.QuerySpeedTest sparkTest-0.0.1-SNAPSHOT.jar /user/zhaogj/input/30G /user/zhaogj/output <br>
 * spark-submit --executor-cores 2 --class com.qzt360.zhaogj.QuerySpeedTest sparkTest-0.0.1-SNAPSHOT.jar /user/zhaogj/input/30G /user/zhaogj/output <br>
 * spark-submit --num-executors 64 --executor-cores 2 --class com.qzt360.zhaogj.QuerySpeedTest sparkTest-0.0.1-SNAPSHOT.jar /user/zhaogj/input/30G /user/zhaogj/output <br>
 */
object QuerySpeedTest {
  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("<input> <output>")
      System.exit(2)
    }
    val sparkConf = new SparkConf().setAppName("SparkShell")
    val sc = new SparkContext(sparkConf)
    val dpi = sc.textFile(args(0))
    val find = dpi.filter(_.contains("1")).filter(_.contains("zgj"))
    find.saveAsTextFile(args(1))
  }
}