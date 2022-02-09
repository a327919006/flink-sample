package com.cn.flink.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

/**
 * union合流，合并多个流，返回结果类型需相同
 *
 * @author Chen Nan
 */
public class Test7_Union {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        File file1 = new File("sample-base\\src\\main\\resources\\hello.txt");
        DataStream<String> fileData = env.readTextFile(file1.getAbsolutePath());

        File file2 = new File("sample-base\\src\\main\\resources\\data.txt");
        env.readTextFile(file2.getAbsolutePath())
                .union(fileData)
                .print();

        env.execute();
    }
}
