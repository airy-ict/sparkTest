package com.qzt360.zhaogj
import org.apache.hadoop.hbase.{ HBaseConfiguration, HTableDescriptor, TableName }
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

import org.apache.spark._

object SparkHBaseGetTest {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("SparkHBaseSpeedTest")
    val sc = new SparkContext(sparkConf)
    val dpi = sc.textFile("/user/zhaogj/input/dpiB.txt")
    //println("dpi count:" + dpi.count)

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "webserver,namenode01,namenode02")
    //Get操作
    val table = new HTable(conf, "tbl_zhaogj")
    val get = new Get(Bytes.toBytes("133"))
    val result = table.get(get)
    println("rowkey:" + new String(result.getRow))
    println("result.value:" + Bytes.toString(result.value))
    println("getValue name:" + Bytes.toString(result.getValue(Bytes.toBytes("nothing"), Bytes.toBytes("name"))))
    println("getValue name1:" + Bytes.toString(result.getValue(Bytes.toBytes("nothing"), Bytes.toBytes("name1"))))

    
    
    //    for (i <- 1 to 2000000) {
    //      val get = new Get(Bytes.toBytes("" + i))
    //      val result = table.get(get)
    //    }
//    var i = 1
//    sc.parallelize(1 to 100000,1).map(line => {
//      val get = new Get(Bytes.toBytes("" + i))
//      val result = table.get(get)
//      i = i + 1
//    })
    //    val tmp = dpi.map(line => {
    //      val conf = HBaseConfiguration.create()
    //      conf.set("hbase.zookeeper.property.clientPort", "2181")
    //      conf.set("hbase.zookeeper.quorum", "webserver,namenode01,namenode02")
    //      val table = new HTable(conf, "tbl_zhaogj")
    //      val fileds = line.split(",")
    //      val get = new Get(Bytes.toBytes("" + fileds.length))
    //      val result = table.get(get)
    //      (Bytes.toString(result.getValue(Bytes.toBytes("nothing"), Bytes.toBytes("name"))), line)
    //      table.close()
    //    })
    //tmp.saveAsTextFile(args(0))
    //    dpi.foreachPartition(x => {
    //      val get = new Get(Bytes.toBytes("" + x.length))
    //      val result = table.get(get)
    //      result.getValue(Bytes.toBytes("nothing"), Bytes.toBytes("name"))
    //    })
    table.close()
    sc.stop()
  }
}