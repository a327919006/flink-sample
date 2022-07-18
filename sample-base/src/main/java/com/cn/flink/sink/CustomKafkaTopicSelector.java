package com.cn.flink.sink;

import org.apache.flink.connector.kafka.sink.TopicSelector;

/**
 * @author Chen Nan
 */
public class CustomKafkaTopicSelector implements TopicSelector<String> {

    @Override
    public String apply(String data) {
        String topic;
        if (data.length() % 2 == 0) {
            topic = "cn_test1";
        } else {
            topic = "cn_test2";
        }
        return topic;
    }
}
