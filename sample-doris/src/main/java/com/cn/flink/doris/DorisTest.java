package com.cn.flink.doris;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.Properties;

/**
 * 从kafka读入数据，写入Doris
 * 数据格式为json
 *
 * #简单样例
 * 1、建立库表
 * CREATE DATABASE example_db;
 * USE example_db;
 * CREATE TABLE table1
 * (
 *     siteid INT DEFAULT '10',
 *     citycode SMALLINT,
 *     username VARCHAR(32) DEFAULT '',
 *     pv BIGINT SUM DEFAULT '0'
 * )
 * AGGREGATE KEY(siteid, citycode, username)
 * DISTRIBUTED BY HASH(siteid) BUCKETS 10
 * PROPERTIES("replication_num" = "1");
 *
 * insert into table1 values(1,1,"test",2);
 * select * from table1;
 *
 * 2、往kafka写入数据
 * {"siteid":2,"citycode":2,"username":"test2","pv":2}
 *
 * 3、查看结果
 * select * from table1;
 *
 * @author Chen Nan
 */
public class DorisTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("192.168.5.132:39573")
                .setProperty("enable.auto.commit", "true")
                .setGroupId("flink-doris-consumer")
                .setTopics("cn_test2")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DorisSink.Builder<String> builder = DorisSink.builder();
        Properties properties = new Properties();
        properties.setProperty("format", "json");
        properties.setProperty("read_json_by_line", "true");
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder.setFenodes("192.168.5.137:8030")
                .setTableIdentifier("example_db.table1")
                .setUsername("root")
                .setPassword("");
        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();
        executionBuilder
                .setStreamLoadProp(properties)
                .setBufferSize(8 * 1024)
                .setBufferCount(3);

        builder.setDorisExecutionOptions(executionBuilder.build())
                .setSerializer(new SimpleStringSerializer())
                .setDorisOptions(dorisBuilder.build());

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "property-kafka-source")
                .process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        System.out.println("收到数据=" + value);
                        out.collect(value);
                    }
                })
                .sinkTo(builder.build());

        env.execute(DorisTest.class.getName());
    }
}
