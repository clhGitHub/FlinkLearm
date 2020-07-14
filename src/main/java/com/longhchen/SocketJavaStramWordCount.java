package com.longhchen;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Create by longhchen on  2020-07-11 19:46
 */
public class SocketJavaStramWordCount {
    public static void main(String[] args) throws Exception {
        //判断参数
//        if(args.length<2){
//            System.err.println("nosocket port hostname");
//            return;
//        }
        //定义变量
//        String hostname = args[0];
//        int port = Integer.parseInt(args[1]);
        //创建环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //transform
        DataStreamSource<String> stream = env.socketTextStream("localhost", 11111);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = stream.flatMap(new LineSplit()).keyBy(0).sum(1);

        sum.print();

        env.execute("socket wordcount");
    }

    private static class LineSplit implements org.apache.flink.api.common.functions.FlatMapFunction<String, Tuple2<String,Integer>> {

        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] tokens = s.toLowerCase().split("\\W+");
            for (String token : tokens) {
                collector.collect(new Tuple2(token,1));
            }
        }
    }
}
