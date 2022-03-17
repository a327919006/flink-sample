package com.cn.flink.helloworld;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 流处理，结果每读一条，输出一次结果
 *
 * @author Chen Nan
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        ParameterTool pt = ParameterTool
                .fromPropertiesFile(BatchWordCount.class.getResourceAsStream("/config.properties"))
                .mergeWith(ParameterTool.fromArgs(args))
                .mergeWith(ParameterTool.fromSystemProperties());

        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从args中获取参数
        String host = pt.get("host", "192.168.157.138");
        int port = pt.getInt("port", 7777);

        // 从socket文本流读取数据，linux服务器运行nc -lk 7777
        DataStream<String> dataSource = env.socketTextStream(host, port);

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
