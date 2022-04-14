package com.cn.flink.tableapi;

import com.cn.flink.domain.SensorData;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.File;

import static org.apache.flink.table.api.Expressions.$;

/**
 * TableAPI和SQL用法
 *
 * @author Chen Nan
 */
public class Test2_CommonAPI {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        File file = new File("sample-base\\src\\main\\resources\\data.txt");
        String filePath = file.getAbsolutePath();

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                // flink1.14前有区分planner和blink版planner，1.14后删除了旧版planner，无需指定planner
                // .useBlinkPlanner()
                // 设置流式处理inStreamingMode或批处理inBatchMode
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.BIGINT())
                .column("name", DataTypes.STRING())
                .column("value", DataTypes.DECIMAL(10, 2))
                .column("timestamp", DataTypes.BIGINT())
                .build();

        TableDescriptor tableDesc = TableDescriptor.forConnector("filesystem")
                .schema(schema)
                .option("path", filePath)
                .format("csv")
                .build();
        tableEnv.createTemporaryTable("inputTable", tableDesc);

        Table inputTable = tableEnv.from("inputTable");
        Table aggTable = inputTable.groupBy($("id"))
                .select($("id").as("did"),
                        $("value").max().as("maxValue"));

        Table sqlAggTable = tableEnv.sqlQuery("select id, max(`value`) as maxVal from inputTable group by id");


        // 输出表结构
        inputTable.printSchema();
        tableEnv.toDataStream(inputTable, SensorData.class).print("input");

        // 聚合操作要使用ChangelogStream，因为聚合结果会改变，每次会输出两条数据，旧值与新值
        tableEnv.toChangelogStream(aggTable).print("agg");
        tableEnv.toChangelogStream(sqlAggTable).print("sqlAgg");

        env.execute();
    }
}
