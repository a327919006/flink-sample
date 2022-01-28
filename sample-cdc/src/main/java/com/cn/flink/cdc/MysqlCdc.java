package com.cn.flink.cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Chen Nan
 */
public class MysqlCdc {

    public static void main(String[] args) throws Exception {
        MySqlSource<String> source = MySqlSource.<String>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("cntest")
                .tableList("cntest.student") // 可选配置项,默认为所有表
                .startupOptions(StartupOptions.initial())
                .deserializer(new StringDebeziumDeserializationSchema())
                .build();

        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "mysql-cdc-source")
                .print()
                .name("test_cdc");

        env.execute();
    }
}
