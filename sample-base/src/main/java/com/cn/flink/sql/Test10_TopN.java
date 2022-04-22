package com.cn.flink.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.File;

/**
 * TopN：flink针对topN场景对OVER优化，支持ORDER自定义列，升序或降序
 * https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/dev/table/sql/queries/topn/
 * https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/dev/table/sql/queries/window-topn/
 *
 * @author Chen Nan
 */
public class Test10_TopN {
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

        // TopN
        Table result = tableEnv.sqlQuery(
                "SELECT id, dataCount, row_num " +
                        "FROM (" +
                        "   SELECT *," +
                        "      ROW_NUMBER() OVER (" +
                        "          ORDER BY dataCount DESC " +
                        "      ) AS row_num" +
                        "   FROM (SELECT id, COUNT(`value`) AS dataCount FROM inputTable GROUP BY id)" +
                        ")" +
                        "WHERE row_num <= 2"
        );

        // 此处要使用toChangelogStream，因为排名是不断变化的，要支持更新
        // tableEnv.toChangelogStream(result)
        //         .print();


        String subQuery = "SELECT id, COUNT(`value`) AS dataCount, window_start, window_end " +
                "FROM TABLE( " +
                "  TUMBLE( TABLE inputTable, DESCRIPTOR(`ts`), INTERVAL '5' SECOND) " +
                ") " +
                "GROUP BY id, window_start, window_end ";

        // 窗口TopN
        Table result2 = tableEnv.sqlQuery(
                "SELECT id, dataCount, row_num, window_end " +
                        "FROM (" +
                        "   SELECT *," +
                        "      ROW_NUMBER() OVER ( " +
                        "          PARTITION BY window_start, window_end " +
                        "          ORDER BY dataCount DESC " +
                        "      ) AS row_num" +
                        "   FROM ( " + subQuery + ")" +
                        ")" +
                        "WHERE row_num <= 2"
        );

        // 此处可以使用toDataStream，因为开窗后窗口内数据不会变化，排名固定
        tableEnv.toDataStream(result2)
                .print();
        env.execute();
    }
}
