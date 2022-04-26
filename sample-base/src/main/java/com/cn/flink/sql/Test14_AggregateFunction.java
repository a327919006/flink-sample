package com.cn.flink.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

import java.io.File;

/**
 * 自定义函数：聚合函数
 * 一条输入，一条输出
 *
 * @author Chen Nan
 */
public class Test14_AggregateFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        File inputFile = new File("sample-base\\src\\main\\resources\\data.txt");
        String inputFilePath = inputFile.getAbsolutePath();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String inputTableSql = "CREATE TABLE inputTable (" +
                "  id BIGINT," +
                "  name STRING," +
                "  `value` DECIMAL(10,2)," +
                "  `time` BIGINT" +
                ") WITH (" +
                "  'connector' = 'filesystem'," +
                "  'path' = '" + inputFilePath + "'," +
                "  'format' = 'csv'" +
                ")";
        tableEnv.executeSql(inputTableSql);

        // 注册全局函数MyDataFunction，并指定实现类
        tableEnv.createTemporarySystemFunction("MyMaxFunction", MyMaxFunction.class);

        // 使用自定义函数进行查询
        Table result = tableEnv.sqlQuery("SELECT id, MyMaxFunction(`value`) AS maxValue " +
                "FROM inputTable " +
                "GROUP BY id"
        );

        tableEnv.toChangelogStream(result)
                .print();
        env.execute();
    }

    public static class MyMaxValue {
        public Double value = 0D;
    }

    /**
     * 这里不能直接使用Double类型的accumulator
     */
    public static class MyMaxFunction extends AggregateFunction<Double, MyMaxValue> {

        @Override
        public MyMaxValue createAccumulator() {
            return new MyMaxValue();
        }

        public void accumulate(MyMaxValue accumulator, Double currValue) {
            if (currValue > accumulator.value) {
                accumulator.value = currValue;
            }
        }

        @Override
        public Double getValue(MyMaxValue accumulator) {
            return accumulator.value;
        }
    }
}
