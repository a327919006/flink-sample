package com.cn.flink.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.File;

/**
 * 事件时间
 *
 * @author Chen Nan
 */
public class Test7_EventTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        File intputFile = new File("sample-base\\src\\main\\resources\\data.txt");
        String intputFilePath = intputFile.getAbsolutePath();
        File outputFile = new File("sample-base\\src\\main\\resources\\result");
        String outputFilePath = outputFile.getAbsolutePath();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 定义水位线，需要时间戳类型字段
        // 先把long类型的time字段通过TO_TIMESTAMP转换为时间戳
        // 但是TO_TIMESTAMP需传入字符串类型，因此通过FROM_UNIXTIME，将time转为字符串类型
        String inputTableSql = "CREATE TABLE inputTable (" +
                "  id BIGINT," +
                "  name STRING," +
                "  `value` DECIMAL(10,2)," +
                "  `time` BIGINT," +
                "  `ts` AS TO_TIMESTAMP( FROM_UNIXTIME(`time` / 1000))," +
                "  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND" +
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
                "  et TIMESTAMP" +
                ") WITH (" +
                "  'connector' = 'filesystem'," +
                "  'path' = '" + outputFilePath + "'," +
                "  'format' = 'json'" +
                ")";

        String insertSql = "INSERT INTO outputTable " +
                "SELECT id, name, `value`, `time`, ts " +
                "FROM inputTable " +
                "WHERE id = 1 ";

        tableEnv.executeSql(inputTableSql);
        tableEnv.executeSql(outputTableSql);
        tableEnv.executeSql(insertSql);
    }
}
