package com.cn.flink.helloworld;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.File;

/**
 * 批处理，结果统一输出
 *
 * @author Chen Nan
 */
public class BatchWordCount {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 从文件中读取数据
        File file = new File("sample-base\\src\\main\\resources\\hello.txt");
        DataSet<String> dataSource = env.readTextFile(file.getAbsolutePath());

        // 对数据集进行处理，按空格分词展开，转换成(word, 1)二元组进行统计
        // 按照第一个位置的word分组
        // 按照第二个位置上的数据求和
        dataSource.flatMap(
                (FlatMapFunction<String, Tuple2<String, Integer>>) (value, out) -> {
                    // value为一行数据
                    String[] words = value.split(" ");

                    for (String word : words) {
                        out.collect(new Tuple2<>(word, 1));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT)) // 指定flatMap输出类型
                .groupBy(0) // 按第一个位置的word分组
                .sum(1)// 按第二个位置的数据求和
                .print();
    }
}
