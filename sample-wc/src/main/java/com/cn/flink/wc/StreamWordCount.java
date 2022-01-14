package com.cn.flink.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Chen Nan
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();

        // 设置并行度，默认值 = 当前计算机的CPU逻辑核数（设置成1即单线程处理）
        env.setParallelism(4);

        // 从文件中读取数据
//        String filePath = "D:\\share\\flink-sample\\sample-wc\\src\\main\\resources\\hello.txt";
//        DataStream<String> dataSource = env.readTextFile(filePath);

        // 从args中获取参数
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host", "192.168.1.222");
        int port = parameterTool.getInt("port", 7777);

        // 从socket文本流读取数据，linux服务器运行nc -lk 7777
        DataStream<String> dataSource = env.socketTextStream(host, port);

        // 基于数据流进行转换计算
        dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");

                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        })
                .keyBy(item -> item.f0)
                .sum(1)
                .print();

        // 执行任务
        env.execute();
    }
}
