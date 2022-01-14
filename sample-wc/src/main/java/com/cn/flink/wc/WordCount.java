package com.cn.flink.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author Chen Nan
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 从文件中读取数据
        String filePath = "D:\\share\\flink-sample\\sample-wc\\src\\main\\resources\\hello.txt";
        DataSet<String> dataSource = env.readTextFile(filePath);

        dataSource.flatMap( // 逐行处理数据
                new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] words = value.split(" ");

                        for (String word : words) {
                            out.collect(new Tuple2<>(word, 1));
                        }
                    }
                })
                .groupBy(0) // 按第一个位置的word分组
                .sum(1)// 按第二个位置的数据求和
                .print();
    }
}
