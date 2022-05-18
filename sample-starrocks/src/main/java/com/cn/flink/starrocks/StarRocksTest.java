package com.cn.flink.starrocks;

import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.table.StarRocksSinkOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * 从kafka读入数据，写入StarRocks
 * 1、往kafka写入测试数据：
 *      {"score":"66","name":"zhangsan"}
 * 2、StarRocks建库
 *      CREATE DATABASE example_db;
 * 3、StarRocks建表
 *      CREATE TABLE IF NOT EXISTS example_db.student_score (
 *          score INT NOT NULL COMMENT "score",
 *          name VARCHAR(100) NOT NULL COMMENT "name")
 *      AGGREGATE KEY(score, name)
 *      DISTRIBUTED BY HASH(score) BUCKETS 10
 *      PROPERTIES("replication_num" = "1");
 * 4、查询结果
 *      select * from example_db.student_score
 *
 * @author Chen Nan
 */
@Slf4j
public class StarRocksTest {

    public static void main(String[] args) throws Exception {

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("192.168.5.132:39573")
                .setProperty("enable.auto.commit", "true")
                .setGroupId("flink-starrocks-consumer")
                .setTopics("test_starrocks")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        SinkFunction<String> sink = StarRocksSink.sink(
                // the sink options
                StarRocksSinkOptions.builder()
                        .withProperty("jdbc-url", "jdbc:mysql://192.168.5.131:19030")
                        .withProperty("load-url", "192.168.5.131:18030")
                        .withProperty("username", "test")
                        .withProperty("password", "123456")
                        .withProperty("database-name", "example_db")
                        .withProperty("table-name", "student_score")
                        .withProperty("sink.properties.format", "json")
                        .withProperty("sink.properties.strip_outer_array", "true")
                        // 刷库间隔
                        .withProperty("sink.buffer-flush.interval-ms", "1000")
                        .build()
        );

        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "property-kafka-source")
                .process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        System.out.println("value=" + value);
                        out.collect(value);
                    }
                })
                .addSink(sink)
                .name("test_starrocks");

        env.execute();
    }
}
