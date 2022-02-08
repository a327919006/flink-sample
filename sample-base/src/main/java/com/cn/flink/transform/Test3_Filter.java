package com.cn.flink.transform;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

/**
 * filter过滤，数据过滤
 *
 * @author Chen Nan
 */
public class Test3_Filter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        File file = new File("sample-base\\src\\main\\resources\\data.txt");
        env.readTextFile(file.getAbsolutePath())
                .filter((FilterFunction<String>) value -> StringUtils.startsWith(value, "1"))
                .print();

        env.execute();
    }
}
