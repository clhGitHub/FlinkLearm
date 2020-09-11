import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.kafka.{KafkaCluster, KafkaUtils}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaToHbase {


  def getOffset(kafkaCluster: KafkaCluster, groupId: String, topic: String) :Map[TopicAndPartition,Long]={

    //11、获取主题分区
    val topicAndPartitions: Either[Err, Set[TopicAndPartition]] = kafkaCluster.getPartitions(Set(topic))

    //12、判断传入的主题是否有分区信息
    if(topicAndPartitions.isRight){

      //13、取出主题分区
      val topicAndPartition = topicAndPartitions.right.get

      //14、获取offset
      kafkaCluster.getConsumerOffsets()



    }

    null
  }

  //3、创建一个方法
  def createFunction(): StreamingContext = {
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
    getOffset(kafkaCluster,groupId,topic)

    //8、读取kafka数据
    KafkaUtils.createDirectStream(
      ssc,
      kafkaParam,
      partitionToLong,
      (messagHandler:MessageAndMetadata[String,String]) => messagHandler.message()
    )



    null
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
