package com.cn.flink.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.common.config.PulsarOptions;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.pulsar.client.api.SubscriptionType;

/**
 * 从Pulsar读取数据
 * <p>
 * https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/connectors/datastream/pulsar/
 *
 * @author Chen Nan
 */
@Slf4j
public class SourceTest5_Pulsar {

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.set(PulsarOptions.PULSAR_STATS_INTERVAL_SECONDS, 1000L);
        PulsarSource<String> source = PulsarSource.builder()
                .setServiceUrl("pulsar://192.168.230.128:6650")
                .setAdminUrl("http://192.168.230.128:8080")
                // 订阅主题，支持多主题和通配符
                .setTopics("test1", "test2")
                // .setTopicPattern("test*")
                .setSubscriptionName("flink-test")
                .setSubscriptionType(SubscriptionType.Exclusive)
                // 消费点位，earliest/latest/fromMessageId/fromMessageTime
                // .setStartCursor(StartCursor.latest())
                .setConfig(config)
                .setDeserializationSchema(PulsarDeserializationSchema.flinkSchema(new SimpleStringSchema()))
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "test-pulsar-source")
                .process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        log.info("收到消息：{}", value);
                        out.collect(value);
                    }
                })
                .print();

        env.execute();
    }
}
