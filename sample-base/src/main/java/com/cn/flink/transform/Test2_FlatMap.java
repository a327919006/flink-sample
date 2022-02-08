package com.cn.flink.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

/**
 * flatMap一对多转换，一个输入可以通过out.collect()返回多个输出
 *
 * @author Chen Nan
 */
public class Test2_FlatMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        File file = new File("sample-base\\src\\main\\resources\\data.txt");
        env.readTextFile(file.getAbsolutePath())
                .flatMap((FlatMapFunction<String, String>) (value, out) -> {
                    String[] split = value.split(",");
                    for (String data : split) {
                        out.collect(data);
                    }
                }, TypeInformation.of(String.class))
                .print();

        env.execute();
    }
}
