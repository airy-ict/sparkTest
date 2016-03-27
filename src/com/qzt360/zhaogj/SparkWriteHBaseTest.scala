package com.qzt360.zhaogj
import org.apache.hadoop.hbase.{ HBaseConfiguration, HTableDescriptor, TableName }
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.HTable
//import org.apache.hadoop.hbase.mapreduce.TableInputFormat
//import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark._

object SparkWriteHBaseTest {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("SparkWriteHBaseTest")
    val sc = new SparkContext(sparkConf)
    //定义 HBase 的配置
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "webserver,namenode01,namenode02")
    //conf.set(TableInputFormat.INPUT_TABLE, "tbl_zhaogj")

    //指定输出格式和输出表名
    val jobConf = new JobConf(conf, this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "tbl_zhaogj")

    for (i <- 1 to 2000000000) {
      //read RDD data from somewhere and convert
      val rawData = List(("" + i, "lilei" + i, i), ("" + (i + 1), "hanmei", 18), ("" + (i + 2), "someone", 38))
      val localData = sc.parallelize(rawData).map(convert)
      localData.saveAsHadoopDataset(jobConf)
    }
    val put = new Put(Bytes.toBytes(""))
    put.addColumn(Bytes.toBytes("nothing"), Bytes.toBytes("name"), Bytes.toBytes(""))
    
  }
  def convert(triple: (String, String, Int)) = {
    val p = new Put(Bytes.toBytes(triple._1))
    p.addColumn(Bytes.toBytes("nothing"), Bytes.toBytes("name"), Bytes.toBytes(triple._2))
    p.addColumn(Bytes.toBytes("nothing"), Bytes.toBytes("age"), Bytes.toBytes(triple._3))
    (new ImmutableBytesWritable, p)
  }
}