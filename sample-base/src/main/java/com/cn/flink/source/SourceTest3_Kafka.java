package com.cn.flink.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.io.File;
import java.util.regex.Pattern;

/**
 * 从Kafka读取数据
 * <p>
 * https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/connectors/datastream/kafka/
 *
 * @author Chen Nan
 */
public class SourceTest3_Kafka {

    public static void main(String[] args) throws Exception {
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("192.168.5.131:39573")
                .setProperty("enable.auto.commit", "true")
                .setGroupId("flink-consumer")
                .setTopics("source_test")
                // 如果使用Pattern，则可配合discovery动态发现新主题，否则消费不到新主题数据，需要重启job
//                .setTopicPattern(Pattern.compile("^(cn_(test)+).*"))
//                .setProperty("partition.discovery.interval.ms", "10000")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "test-kafka-source")
                .print();

        env.execute();
    }
}
