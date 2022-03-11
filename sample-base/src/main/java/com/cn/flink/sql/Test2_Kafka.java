package com.cn.flink.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.File;

/**
 * TableAPI和SQL入门示例
 *
 * @author Chen Nan
 */
public class Test2_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String inputTableSql = "CREATE TABLE inputTable (" +
                "  id BIGINT," +
                "  name STRING," +
                "  `value` DECIMAL(10,2)," +
                "  `time` BIGINT" +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'sensorInput'," +
                "  'properties.bootstrap.servers' = 'localhost:9092'," +
                "  'properties.group.id' = 'flinkTest'," +
                "  'scan.startup.mode' = 'earliest-offset'," +
                "  'format' = 'csv'" +
                ")";
        String outputTableSql = "CREATE TABLE outputTable (" +
                "  did BIGINT," +
                "  deviceName STRING," +
                "  `value` DECIMAL(10,2)," +
                "  `time` BIGINT" +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'sensorOutput'," +
                "  'properties.bootstrap.servers' = 'localhost:9092'," +
                "  'format' = 'json'" +
                ")";

        String insertSql = "INSERT INTO outputTable " +
                "SELECT id, name, `value`, `time` " +
                "FROM inputTable " +
                "WHERE id = 1 ";

        tableEnv.executeSql(inputTableSql);
        tableEnv.executeSql(outputTableSql);
        tableEnv.executeSql(insertSql);
    }
}
