package com.cn.flink.transform;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

/**
 * 重分区，不常用
 *
 * @author Chen Nan
 */
public class Test9_Partition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        File file = new File("sample-base\\src\\main\\resources\\data.txt");
        DataStreamSource<String> dataStream = env.readTextFile(file.getAbsolutePath());

        dataStream.shuffle().print("shuffle");
        dataStream.global().print("global");

        dataStream.print("input");

        env.execute();
    }
}
