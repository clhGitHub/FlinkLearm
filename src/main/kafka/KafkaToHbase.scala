import java.lang
import java.util.{Date, Properties}

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.kafka.{KafkaCluster, KafkaUtils,HasOffsetRanges,OffsetRange}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.immutable.HashMap
import utils.{DateUtils, HBaseUtils, JsonUtils, StartupReportLogs}
object KafkaToHbase {


  def getOffset(kafkaCluster: KafkaCluster, groupId: String, topic: String) :Map[TopicAndPartition,Long]={
    //16、声明一个主题分区对应的offset
    var partitionToLong = new HashMap[TopicAndPartition, Long]()

    //11、获取主题分区
    val topicAndPartitions: Either[Err, Set[TopicAndPartition]] = kafkaCluster.getPartitions(Set(topic))

    //12、判断传入的主题是否有分区信息
    if(topicAndPartitions.isRight){

      //13、取出主题分区
      val topicAndPartition: Set[TopicAndPartition] = topicAndPartitions.right.get

      //14、获取offset
      val topicAndPartitionToOffsets: Either[Err, Map[TopicAndPartition, Long]] = kafkaCluster.getConsumerOffsets(groupId, topicAndPartition)

      //15、判断是否消费过
      if(topicAndPartitionToOffsets.isLeft){
        //没有消费过，赋值0，从头消费
        for (tp <- topicAndPartition) {
          partitionToLong += (tp -> 0)
        }
      }else{
        //消费过数据，取出其中的offset
        val topicAndPartitionToOffset: Map[TopicAndPartition, Long] = topicAndPartitionToOffsets.right.get
        partitionToLong ++= topicAndPartitionToOffset
      }
    }
    partitionToLong
  }

  def setOffset(groupId: String, kafkaCluster: KafkaCluster, kafkaStream: InputDStream[String]): Unit = {
    kafkaStream.foreachRDD(rdd => {
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      for (offsetRange <- offsetRanges) {
        //获取当前主题以及分区
        val topicAndPartition: TopicAndPartition = offsetRange.topicAndPartition()

        //取出offset
        val offset: Long = offsetRange.untilOffset

        //提交offset
        kafkaCluster.setConsumerOffsets(groupId,Map(topicAndPartition->offset))
      }
    })
  }

  //3、创建一个方法
  def createFunction(): StreamingContext = {
    val properties = new Properties()
    //4、创建sparkconf
    val sparkConf = new SparkConf().setAppName("kafka2hbse").setMaster("local[*]")

    // 6.配置Spark Streaming查询1s从kafka一个分区消费的最大的message数量
    sparkConf.set("spark.streaming.maxRatePerPartition", "100")

    // 7.配置Kryo序列化方式
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryo.registrator", "com.atguigu.registrator.MyKryoRegistrator")

    //5、获取程序入口
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    
    //9、kafkaParam 封装参数
    val topic = "log-analysis"
    val groupId = "g1"
    val kafkaParam = Map(
      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      "group.id" -> groupId
    )
    //10、获取offset
    val kafkaCluster = new KafkaCluster(kafkaParam)
    val partitionToLong: Map[TopicAndPartition, Long] = getOffset(kafkaCluster, groupId, topic)

    //8、读取kafka数据
    val kafkaStream: InputDStream[String] = KafkaUtils.createDirectStream(
      ssc,
      kafkaParam,
      partitionToLong,
      (messagHandler: MessageAndMetadata[String, String]) => messagHandler.message()
    )
    //16、写入hbase
    kafkaStream.foreachRDD(rdd =>{
      rdd.foreach(str => {
        println("**************************")
        //将json数据转为对象
        val logs: StartupReportLogs = JsonUtils.json2StartupLog(str)
        //取出城市和启动时间
        val ts: lang.Long = logs.getStartTimeInMs
        val city: String = logs.getCity

        val time: String = DateUtils.dateToString(new Date(ts))

        //拼接rowkey
        val rowkey: String = city + time

        //获取HBase表对象，这行代码最好放到外面，不然每条数据都要创建一个表对象
        val table: Table = HBaseUtils.getHBaseTabel(properties)

        //向表中添加数据
        table.incrementColumnValue(Bytes.toBytes(rowkey), Bytes.toBytes("info"), Bytes.toBytes("count"), 1L)
      })
    })
    //将offset提交
    setOffset(groupId,kafkaCluster,kafkaStream)
    ssc
  }

  def main(args: Array[String]): Unit = {
    // 2、设置checkpoint 目录
    val ckPath: String = "ck/kafkaToBase"

    //1、回复或重新创建ssc
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate(ckPath, () => createFunction())

    //启动ssc
    ssc.start()
    ssc.awaitTermination()
  }

}
