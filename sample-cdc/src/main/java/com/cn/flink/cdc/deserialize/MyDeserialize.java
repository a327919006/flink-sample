package com.cn.flink.cdc.deserialize;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * @author Chen Nan
 */
@Slf4j
public class MyDeserialize implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        String topic = sourceRecord.topic();
        String[] arr = topic.split("\\.");
        String db = arr[1];
        String tableName = arr[2];

        //获取操作类型 READ DELETE UPDATE CREATE
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        log.info("db={} tableName={} operation={}", db, tableName, operation);

        //获取值信息并转换为 Struct 类型
        Struct value = (Struct) sourceRecord.value();
        //获取变化后的数据
        Struct after = value.getStruct("after");
        //创建 JSON 对象用于存储数据信息
        JSONObject data = new JSONObject();
        for (Field field : after.schema().fields()) {
            Object o = after.get(field);
            data.put(field.name(), o);
        }
        //创建 JSON 对象用于封装最终返回值数据信息
        JSONObject result = new JSONObject();
        result.put("operation", operation.toString().toLowerCase());
        result.put("data", data);
        result.put("database", db);
        result.put("table", tableName);
        //发送数据至下游
        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
