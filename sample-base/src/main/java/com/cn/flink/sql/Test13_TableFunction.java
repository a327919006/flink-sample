package com.cn.flink.sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;

import java.io.File;

/**
 * 自定义函数：表函数
 * 一条输入，多条输出
 *
 * @author Chen Nan
 */
public class Test13_TableFunction {
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
        tableEnv.createTemporarySystemFunction("MyDataFunction", MyDataFunction.class);

        // 使用自定义函数进行查询
        Table result = tableEnv.sqlQuery("SELECT id, `value`, " +
                "newName, newTime " +
                "FROM inputTable, " +
                " LATERAL TABLE(MyDataFunction(name, `time`)) AS T(newName, newTime)");

        tableEnv.toDataStream(result)
                .print();
        env.execute();
    }

    public static class MyDataFunction extends TableFunction<Tuple2<String, Long>> {
        /**
         * 一行转换为多行
         */
        public void eval(String name, Long time) {
            collect(Tuple2.of(name, time));
            collect(Tuple2.of(name, System.currentTimeMillis()));
        }
    }
}
