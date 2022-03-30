package com.cn.flink.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

/**
 * 从文件读取数据
 * <p>
 * https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/dev/datastream/overview/
 *
 * @author Chen Nan
 */
public class SourceTest2_FileBatch {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        File file = new File("sample-base\\src\\main\\resources\\hello.txt");
        env.readTextFile(file.getAbsolutePath())
                .print();

        env.execute();
    }
}
