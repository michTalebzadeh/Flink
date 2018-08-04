import java.util.Properties
import java.util.Arrays
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala

import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
//import org.apache.flink.api.scala._ 

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

//import org.apache.kafka.clients.consumer.ConsumerConfig
//import org.apache.kafka.clients.consumer.ConsumerRecord
//import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.TimeCharacteristic

import org.slf4j.LoggerFactory
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
/*
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.HTable
*/
import java.util.Calendar
//import scala.math._
import java.util.Date
import java.text.DateFormat
import java.text.SimpleDateFormat
import org.apache.log4j.Logger
import org.apache.log4j.Level
import sys.process.stringSeqToProcess
//import scala.collection.mutable.HashMap
import java.io.File

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.java.StreamTableEnvironment


import org.apache.flink.types.Row

object md_streaming
{
  private var zookeeperUrl = "rhes75:2181"
  private var requestConsumerId = null
  private var impressionConsumerId = null
  private var clickConsumerId = null
  private var conversionConsumerId = null
  private var requestTopicName = null
  private var impressionTopicName = null
  private var clickTopicName = null
  private var conversionTopicName = null
  private var requestThreads = 0
  private var impressionThreads = 0
  private var clickThreads = 0
  private var conversionThreads = 0
  private var flinkAppName = "md_streaming"
  private var bootstrapServers = "rhes75:9092, rhes75:9093, rhes75:9094"
  private var schemaRegistryURL = "http://rhes75:8081"
  private var zookeeperConnect = "rhes75:2181"
  private var zookeeperConnectionTimeoutMs = "10000"
  private var rebalanceBackoffMS = "15000"
  private var zookeeperSessionTimeOutMs = "15000"
  private var autoCommitIntervalMS = "12000"
  private var topicsValue = "final"
  private var memorySet = "F"
  private var enableHiveSupport = null
  private var enableHiveSupportValue = "true"
  private var sparkStreamingReceiverMaxRateValue = "0"
  private var checkpointdir = "/checkpoint"
  private var hbaseHost = "rhes75"
  private var zookeeperHost = "rhes564"
  private var zooKeeperClientPort = "2181"
  private var writeDirectory = "hdfs://rhes75:9000/tmp/flink/"
  private var fileName = "md.streaming.txt"
  private val maxServingDelay = 60
  private val servingSpeedFactor = 600f
  private var batchInterval = 2
  private val countWindowLength = 4 // window size in sec
  private val countWindowFrequency =  2   // window trigger interval in sec
  private val earlyCountThreshold = 50
  private val writeToElasticsearch = false // set to true to write results to Elasticsearch
  private val elasticsearchHost = "" // look-up hostname in Elasticsearch log output
  private val elasticsearchPort = 9300
  private val logger = LoggerFactory.getLogger(getClass)


  def main(args: Array[String])
  {
   var startTimeQuery = System.currentTimeMillis
/*
    // Start Hbase table stuff
    val tableName = "MARKETDATAHBASESPEEDFLINK"
    val hbaseConf = HBaseConfiguration.create()
//  Connecting to remote Hbase
    hbaseConf.set("hbase.master", hbaseHost)
    hbaseConf.set("hbase.zookeeper.quorum",zookeeperHost)
    hbaseConf.set("hbase.zookeeper.property.clientPort",zooKeeperClientPort)
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
    // create this table with column family
    val admin = new HBaseAdmin(hbaseConf)
    if(!admin.isTableAvailable(tableName))
    {
      println("Creating table " + tableName)
      val tableDesc = new HTableDescriptor(tableName)
      tableDesc.addFamily(new HColumnDescriptor("PRICE_INFO".getBytes()))
      tableDesc.addFamily(new HColumnDescriptor("OPERATION".getBytes()))
      admin.createTable(tableDesc)
    } else {
      println("Table " + tableName + " already exists!!")
    }
    val HbaseTable = new HTable(hbaseConf, tableName)
    // End Hbase table stuff
*/
    var sqltext = ""
    var totalPrices: Long = 0
    val runTime = 240

    val getCheckpointDirectory = new getCheckpointDirectory
    println("Hdfs directory is " + getCheckpointDirectory.checkpointDirectory(flinkAppName))

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", bootstrapServers)
    properties.setProperty("zookeeper.connect", zookeeperConnect)
    properties.setProperty("group.id", flinkAppName)
    properties.setProperty("auto.offset.reset", "latest")
    val  streamExecEnv = StreamExecutionEnvironment.getExecutionEnvironment
     //////streamExecEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // generate a Watermark every two seconds
     ////////streamExecEnv.getConfig().setAutoWatermarkInterval(2000)
    //    
    //Create a Kafka consumer
    //
    val dataStream =  streamExecEnv
       .addSource(new FlinkKafkaConsumer011[String](topicsValue, new SimpleStringSchema(), properties))
 //
 //
    val newDateStream = dataStream.map(new MapFunction[String, Tuple4[String, String, String, String]] {
      override def map(value: String): Tuple4[String, String, String, String] = {
        return null
      }
    })

  val tableEnv = TableEnvironment.getTableEnvironment(streamExecEnv)
    tableEnv.registerDataStream("priceTable", newDateStream, 'key, 'ticker, 'timeissued, 'price)


  sqltext  = "SELECT key from priceTable";
  val result = tableEnv.sql(sqltext);


    val topics = Set(topicsValue)
    val comma = ","
    val q = """""""
    var current_timestamp = "current_timestamp"
    // Work on every Stream
         println("Current time is: " + Calendar.getInstance.getTime)
         // Combine each partition's results into a single RDD
         var endTimeQuery = System.currentTimeMillis
         //println("TotalPrices  streamed in so far: " +totalPrices+ " , Runnig for  " + (endTimeQuery - startTimeQuery)/(1000*60)+" Minutes")
         // Check if running time > runTime exit
         if( (endTimeQuery - startTimeQuery)/(100000*60) > runTime)
         {
           println("\nDuration exceeded " + runTime + " minutes exiting")
           System.exit(0)
         }
/*
           var key =  dataStream.split(',').view(0).toString
           var ticker =   dataStream.split(',').view(1).toString
           var timeissued =  dataStream.split(',').view(2).toString
           var price =  dataStream.split(',').view(3).toFloat
           val priceToString =  dataStream.split(',').view(3)
           val CURRENCY = "GBP"
           val op_type = "1"
           val op_time = System.currentTimeMillis.toString
           println("\nkey is " + key)
*/
/*
           if (price > 90.0)
           {
             println ("price > 90.0, saving to Hbase table!")

            // Save prices to Hbase table
             var p = new Put(new String(key).getBytes())
             //p.add("PRICE_INFO".getBytes(), "key".getBytes(),          new String(ticker).getBytes())
             p.add("PRICE_INFO".getBytes(), "TICKER".getBytes(),          new String(ticker).getBytes())
             p.add("PRICE_INFO".getBytes(), "SSUED".getBytes(),     new String(timeissued).getBytes())
             p.add("PRICE_INFO".getBytes(), "PRICE".getBytes(),           new String(priceToString).getBytes())
             p.add("PRICE_INFO".getBytes(), "CURRENCY".getBytes(),         new String(CURRENCY).getBytes())
             p.add("OPERATION".getBytes(), "OP_TYPE".getBytes(),         new String(1.toString).getBytes())
             p.add("OPERATION".getBytes(), "OP_TIME".getBytes(),         new String(System.currentTimeMillis.toString).getBytes())
             HbaseTable.put(p)
             HbaseTable.flushCommits()
             if(ticker == "VOD" && price > 99.0)
             {
               sqltext = Calendar.getInstance.getTime.toString + ", Price on "+ticker+" hit " +price.toString
               //java.awt.Toolkit.getDefaultToolkit().beep()
               println(sqltext)
             }
           }
         }
*/

    val sink =  dataStream.writeAsText(writeDirectory+fileName+Calendar.getInstance.getTime.toString, FileSystem.WriteMode.OVERWRITE)
    //env.execute("Flink Kafka Example writing to "+writeDirectory+fileName)
     streamExecEnv.execute("Flink simple output")
  }
}

class getCheckpointDirectory {
  def checkpointDirectory (ProgramName: String) : String  = {
     var hdfsDir = "hdfs://rhes75:9000/user/hduser/"+ProgramName
     return hdfsDir
  }

}
