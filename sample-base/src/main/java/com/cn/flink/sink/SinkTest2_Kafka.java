package com.cn.flink.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

/**
 * sink-将数据写入Kafka
 *
 * @author Chen Nan
 */
public class SinkTest2_Kafka {

    public static void main(String[] args) throws Exception {
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("192.168.5.131:39573")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        // 固定topic
                        //.setTopic("sink_test")
                        // 动态topic，根据数据动态指定topic
                        .setTopicSelector(new CustomKafkaTopicSelector())
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        File file = new File("sample-base\\src\\main\\resources\\hello.txt");
        env.readTextFile(file.getAbsolutePath())
                .sinkTo(sink);

        env.execute();
    }
}
