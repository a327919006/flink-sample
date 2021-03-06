package com.cn.flink.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.File;

/**
 * 开窗：对每一行数据都会计算一次结果
 * https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/dev/table/sql/queries/over-agg/
 * 按id分组，按时间列升序（目前只支持按时间类型列升序）
 * 统计当前行及其前三行数据的平均时间（共4行），起始时不足4行，有多少算多少。
 *
 * @author Chen Nan
 */
public class Test9_Over {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        File intputFile = new File("sample-base\\src\\main\\resources\\data_window.txt");
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
                "SELECT id, AVG(`time`) OVER( " +
                        "   PARTITION BY id " +
                        "   ORDER BY ts" +
                        "   ROWS BETWEEN 3 PRECEDING AND CURRENT ROW " +
                        " ) AS avg_ts " +
                        " FROM inputTable "
        );

        tableEnv.toDataStream(result, Row.class)
                .print();

        env.execute();
    }
}
