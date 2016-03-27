package com.qzt360.zhaogj
import org.apache.hadoop.hbase.{ HBaseConfiguration, HTableDescriptor, TableName }
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

import org.apache.spark._

object SparkHBaseSpeedTest {
  def main(args: Array[String]) {

    // create 'tbl_zhaogj',{NAME => 'nothing'},{SPLITS => ['0','1','2','3','4','5','6','7','8','9']}
    //val sparkConf = new SparkConf().setAppName("SparkHBaseSpeedTest")
    //val sc = new SparkContext(sparkConf)
    val sc = new SparkContext
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "webserver,namenode01,namenode02")
    conf.set(TableInputFormat.INPUT_TABLE, "tbl_zhaogj")

    //Put操作
    val table = new HTable(conf, "tbl_zhaogj")
    for (i <- 1 to 2000000000) {
      var put = new Put(Bytes.toBytes("" + i))
      put.add(Bytes.toBytes("nothing"), Bytes.toBytes("name"), Bytes.toBytes("value " + 10 * i))
      table.put(put)
    }
    table.flushCommits()

    //Get操作
    //    val get = new Get(Bytes.toBytes("1"))
    //    val result = table.get(get)
    //    println("rowkey:" + new String(result.getRow))
    //    println("value:" + Bytes.toString(result.value))
    //    println("value:" + Bytes.toString(result.getValue(Bytes.toBytes("nothing"), Bytes.toBytes("name1"))))

    //    for (i <- 1 to 2000000000) {
    //      val get = new Get(Bytes.toBytes("" + i % 10))
    //      val result = table.get(get)
    //    }

    table.close()

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    println("HBase RDD Count:" + hBaseRDD.count)
    println("HBase RDD Collect:" + hBaseRDD.collect)
    sc.stop

  }
}