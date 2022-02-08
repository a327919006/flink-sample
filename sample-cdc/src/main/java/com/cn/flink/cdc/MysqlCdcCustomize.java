package com.cn.flink.cdc;

import com.cn.flink.cdc.deserialize.MysqlDebeziumDeserialize;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * 自定义序列化方式
 * 将数据写入kafka
 *
 * @author Chen Nan
 */
public class MysqlCdcCustomize {

    public static void main(String[] args) throws Exception {
        MySqlSource<String> source = MySqlSource.<String>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("cntest")
                .tableList("cntest.user") // 可选配置项,默认为所有表
                .startupOptions(StartupOptions.initial())
                .deserializer(new MysqlDebeziumDeserialize()) // 自定义反序列化
                .build();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        FlinkKafkaProducer<String> sink = new FlinkKafkaProducer<>(
                "etl_order_charge", new SimpleStringSchema(), properties);

        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "mysql-cdc-source")
                .addSink(sink)
                .name("test_cdc");

        env.execute();
    }
}
