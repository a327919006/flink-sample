package com.cn.flink.tableapi;

import com.cn.flink.domain.SensorData;
import com.cn.flink.source.MySourceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.File;

import static org.apache.flink.table.api.Expressions.$;

/**
 * TableAPI和SQL入门示例
 *
 * @author Chen Nan
 */
public class Test1_Hello {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensorData> dataStream = env.addSource(new MySourceFunction());

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table table = tableEnv.fromDataStream(dataStream);
        Table resultTable = table
                .select($("id"),
                        $("name"),
                        $("value"),
                        $("timestamp"))
                .where($("id").isEqual(1));

        tableEnv.createTemporaryView("sensor", table);
        String sql = "select id, name, `value` from sensor where id=2";
        Table resultSqlTable = tableEnv.sqlQuery(sql);


        tableEnv.toDataStream(resultTable, SensorData.class).print("resultTable");
        tableEnv.toDataStream(resultSqlTable, Row.class).print("resultSqlTable");
        env.execute();
    }
}
