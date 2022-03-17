package com.cn.flink.helloworld;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

/**
 * 有界流处理，结果每读一条，输出一次结果
 *
 * @author Chen Nan
 */
public class BoundedStreamWordCount {
    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件中读取数据
        File file = new File("sample-base\\src\\main\\resources\\hello.txt");
        DataStream<String> dataSource = env.readTextFile(file.getAbsolutePath());

        // 基于数据流进行转换计算，每一步都支持设置并行度
        dataSource.flatMap(
                (FlatMapFunction<String, Tuple2<String, Integer>>) (value, out) -> {
                    String[] words = value.split(" ");

                    for (String word : words) {
                        out.collect(new Tuple2<>(word, 1));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT)) // 指定flatMap输出类型
                .keyBy(item -> item.f0)
                .sum(1).setParallelism(1)
                .print().setParallelism(1);

        // 执行任务
        env.execute("StreamWordCount");
    }
}
