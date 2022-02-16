package com.cn.flink.transform;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.File;

/**
 * join合流
 *
 * @author Chen Nan
 */
public class Test7_Join {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        File file1 = new File("sample-base\\src\\main\\resources\\hello.txt");
        DataStream<String> fileData = env.readTextFile(file1.getAbsolutePath());

        File file2 = new File("sample-base\\src\\main\\resources\\data.txt");
        env.readTextFile(file2.getAbsolutePath())
                .join(fileData)
                .where(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String value) throws Exception {
                        return value;
                    }
                })
                .equalTo(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String value) throws Exception {
                        return value;
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply(new JoinFunction<String, String, String>() {
                    @Override
                    public String join(String first, String second) throws Exception {
                        return first + second;
                    }
                })
                .print();

        env.execute();
    }
}
