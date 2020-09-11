import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.*;


public class KafkacoConsumer {
    private static String hdfsPath = "hdfs://hadoop102:9000";
    private static String user = "atguigu";
    private static FSDataOutputStream outputStream = null;
    private static FileSystem fs = null;

    public static void main(String[] args) throws URISyntaxException, IOException {
        //创建配置信息
        Properties properties = new Properties();

        //添加配置
        properties.put("zookeeper.connect", "192.168.10.200:2181");
        properties.put("group.id", "group1");
        properties.put("zookeeper.session.timeout.ms", "1000");
        properties.put("zookeeper.sync.time.ms", "250");
        properties.put("auto.commit.interval.ms", "1000");//自动提交

        //创建消费者连接器
        //消费者客户端会通过消费者连接器（ConsumerConnector）连接zk集群
        //获取分配的分区，创建每个分区对应消息流(MessageStream),最后迭代消息流，读取每条消息
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));

        //创建一个topicCountMap（主题&消费该主题的线程数）
        HashMap<String, Integer> topicCountMap = new HashMap<String,Integer>();
        topicCountMap.put("log-analysis",1);

        //获取数据流
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumerConnector.createMessageStreams(topicCountMap);

        //根据主题以及线程顺序获取数据流
        KafkaStream<byte[], byte[]> stream = messageStreams.get("log-analysis").get(0);

        //获取流里面的每一条消息
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();

        //获取当前时间
        long ts = System.currentTimeMillis();

        //创建一个时间格式化对象
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM-dd HH:mm");

        //获取时间存入的路径
        String totalPath = getPath(ts, sdf);
        //获取HDFS文件系统
        try {
            fs = FileSystem.get(new URI(hdfsPath), new Configuration(), user);
            //获取输入流
            if(fs.exists(new Path(totalPath))){
                outputStream = fs.append(new Path(totalPath));
            }else{
                fs.create(new Path(totalPath));
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        while (iterator.hasNext()){
            //获取一行数据
            String log = new String(iterator.next().message());
            //获取当前时间
            long currentTs = System.currentTimeMillis();
            if(currentTs - ts>=60000){
                String currentPath = getPath(currentTs, sdf);
                outputStream.close();
                outputStream = fs.create(new Path(currentPath));

                //更新ts
                ts=currentTs;
            }
            System.out.println(log);
            //写入hdfs按分钟存一个文件
            outputStream.write((log + "\r\n").getBytes());
            outputStream.hsync();
        }

    }

    private static String getPath(long ts, SimpleDateFormat sdf) {
        String formatTime = sdf.format(new Date(ts));
        String[] splits = formatTime.split(" ");

        //取出相应的字段
        String yearMonthDay = splits[0];
        String hourMini = splits[1];
        String parentName = yearMonthDay.replaceAll("-", "/");
        String fileName = hourMini.replaceAll(":", "");

        return  hdfsPath + "/" + parentName + "/" + fileName;


    }
}
