package com.cn.flink.helloworld;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

/**
 * 默认为有界流处理，结果每读一条，输出一次结果
 * 如果设置为批处理模式，则和DataSet效果相同，结果一次性输出
 *
 * @author Chen Nan
 */
public class BoundedStreamWordCount {
    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();
        // 设置并行度，默认值 = 当前计算机的CPU逻辑核数（设置成1即单线程处理）
        // 如果设置为多并行度，则同一个word会被分配到相同的slot处理
        env.setParallelism(1);

        // 设置处理模式，默认为流处理，可设置为批处理，不建议在代码中设置模式
        // 建议使用启动命令设置bin/flink run -Dexecution.runtime-mode=BATCH ...
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // 从文件中读取数据
        File file = new File("sample-base\\src\\main\\resources\\hello.txt");
        DataStream<String> dataSource = env.readTextFile(file.getAbsolutePath());

        dataSource.flatMap(
                (FlatMapFunction<String, Tuple2<String, Integer>>) (value, out) -> {
                    String[] words = value.split(" ");

                    for (String word : words) {
                        out.collect(new Tuple2<>(word, 1));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT)) // 指定flatMap输出类型
                .keyBy(item -> item.f0)
                .sum(1)
                .print();

        // 执行任务
        env.execute("StreamWordCount");
    }
}
