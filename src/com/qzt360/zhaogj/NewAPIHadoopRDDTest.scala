package com.qzt360.zhaogj
import org.apache.hadoop.hbase.{ HBaseConfiguration, HTableDescriptor, TableName }
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark._

object NewAPIHadoopRDDTest {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("NewAPIHadoopRDDTest")
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "webserver,namenode01,namenode02")
    conf.set(TableInputFormat.INPUT_TABLE, "tbl_zhaogj")

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    //println("HBase RDD Count:" + hBaseRDD.count)
    //val rddCount = hBaseRDD.count
    val res = hBaseRDD.take(62230)
    //    res.foreach {
    //      case (_, result) =>
    //        val key = Bytes.toString(result.getRow)
    //        val name = Bytes.toString(result.getValue("nothing".getBytes, "name".getBytes))
    //        println(key + name)
    //    }
    val tmp = res.filter {
      case (_, result) =>
        var find = false
        val key = Bytes.toString(result.getRow)
        if ("10" == key) {
          find = true
        }
        find
    }
    tmp.foreach {
      case (_, result) =>
        val key = Bytes.toString(result.getRow)
        val name = Bytes.toString(result.getValue("nothing".getBytes, "name".getBytes))
        println(key + name)
    }
    //    for (j <- 1 until 10000) {
    //      println("j: " + j)
    //      var rs = res(j - 1)._2
    //      var kvs = rs.raw
    //      for (kv <- kvs)
    //        println("rowkey:" + new String(kv.getRow()) +
    //          " cf:" + new String(kv.getFamily()) +
    //          " column:" + new String(kv.getQualifier()) +
    //          " value:" + new String(kv.getValue()))
    //    }
    sc.stop
  }
}