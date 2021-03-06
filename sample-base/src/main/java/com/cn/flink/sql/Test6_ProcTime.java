package com.cn.flink.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.File;

/**
 * 获取proctime，处理时间
 *
 * @author Chen Nan
 */
public class Test6_ProcTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        File intputFile = new File("sample-base\\src\\main\\resources\\data.txt");
        String intputFilePath = intputFile.getAbsolutePath();
        File outputFile = new File("sample-base\\src\\main\\resources\\result");
        String outputFilePath = outputFile.getAbsolutePath();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String inputTableSql = "CREATE TABLE inputTable (" +
                "  id BIGINT," +
                "  name STRING," +
                "  `value` DECIMAL(10,2)," +
                "  `time` BIGINT," +
                "  pt AS PROCTIME()" +
                ") WITH (" +
                "  'connector' = 'filesystem'," +
                "  'path' = '" + intputFilePath + "'," +
                "  'format' = 'csv'" +
                ")";
        String outputTableSql = "CREATE TABLE outputTable (" +
                "  did BIGINT," +
                "  deviceName STRING," +
                "  `value` DECIMAL(10,2)," +
                "  `time` BIGINT," +
                "  proctime TIMESTAMP" +
                ") WITH (" +
                "  'connector' = 'filesystem'," +
                "  'path' = '" + outputFilePath + "'," +
                "  'format' = 'json'" +
                ")";

        String insertSql = "INSERT INTO outputTable " +
                "SELECT id, name, `value`, `time`, pt " +
                "FROM inputTable " +
                "WHERE id = 1 ";

        tableEnv.executeSql(inputTableSql);
        tableEnv.executeSql(outputTableSql);
        tableEnv.executeSql(insertSql);
    }
}
