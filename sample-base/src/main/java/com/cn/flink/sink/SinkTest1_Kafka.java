package com.cn.flink.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

/**
 * @author Chen Nan
 */
public class SinkTest1_Kafka {

    public static void main(String[] args) throws Exception {
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("127.0.0.1:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("sink_test")
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
