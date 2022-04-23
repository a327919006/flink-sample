package com.cn.flink.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.File;

/**
 * join，同stream的join
 * https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/dev/table/sql/queries/joins/
 * 本示例为InnerJoin，另外支持LeftJoin、RightJoin、FullOuterJoin以及IntervalJoin
 *
 * @author Chen Nan
 */
public class Test11_Join {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        File leftDataFile = new File("sample-base\\src\\main\\resources\\data.txt");
        String leftDatPath = leftDataFile.getAbsolutePath();
        File rightDataFile = new File("sample-base\\src\\main\\resources\\data_window.txt");
        String rightDataPath = rightDataFile.getAbsolutePath();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String leftTableSql = "CREATE TABLE leftTable (" +
                "  id BIGINT," +
                "  name STRING," +
                "  `value` DECIMAL(10,2)," +
                "  `time` BIGINT" +
                ") WITH (" +
                "  'connector' = 'filesystem'," +
                "  'path' = '" + leftDatPath + "'," +
                "  'format' = 'csv'" +
                ")";
        String rightTableSql = "CREATE TABLE rightTable (" +
                "  id BIGINT," +
                "  name STRING," +
                "  `value` DECIMAL(10,2)," +
                "  `time` BIGINT" +
                ") WITH (" +
                "  'connector' = 'filesystem'," +
                "  'path' = '" + rightDataPath + "'," +
                "  'format' = 'csv'" +
                ")";

        String selectSql = "SELECT * " +
                "FROM leftTable " +
                "INNER JOIN rightTable " +
                "ON leftTable.id = rightTable.id " +
                "WHERE rightTable.id = 2";

        tableEnv.executeSql(leftTableSql);
        tableEnv.executeSql(rightTableSql);
        Table tableResult = tableEnv.sqlQuery(selectSql);

        tableEnv.toDataStream(tableResult)
                .print();

        env.execute();
    }
}
