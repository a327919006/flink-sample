package com.cn.flink.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.File;

/**
 * 事件时间
 *
 * @author Chen Nan
 */
public class Test8_Windows {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        File intputFile = new File("sample-base\\src\\main\\resources\\data.txt");
        String intputFilePath = intputFile.getAbsolutePath();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
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
        tableEnv.executeSql(inputTableSql);

        Table result = tableEnv.sqlQuery(
                // 旧版
                // "SELECT " +
                //         "id, " +
                //         "TUMBLE_END(ts, INTERVAL '5' SECOND) as endT, " +
                //         "MAX(`value`) AS mv " +
                //         "FROM inputTable " +
                //         "GROUP BY " +                     // 使用窗口和ID进行分组
                //         "id, " +
                //         "TUMBLE(`ts`, INTERVAL '5' SECOND)" // 定义滚动窗口

                // 新版
                "SELECT " +
                        "id, " +
                        "window_end AS endT, " +
                        "MAX(`value`) AS mv " +
                        "FROM TABLE( " +
                        "TUMBLE( TABLE inputTable, " +
                        "DESCRIPTOR(`ts`), " +
                        "INTERVAL '5' SECOND)) " +
                        "GROUP BY id, window_start, window_end "

        );

        tableEnv.toDataStream(result, Row.class)
                .print();

        env.execute();
    }
}
