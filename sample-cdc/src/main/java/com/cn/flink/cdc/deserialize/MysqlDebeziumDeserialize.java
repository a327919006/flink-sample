package com.cn.flink.cdc.deserialize;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class MysqlDebeziumDeserialize implements DebeziumDeserializationSchema<String> {

    private static final long serialVersionUID = 7906905121308228264L;
    private static final String FORMAT = "yyyy-MM-dd HH:mm:ss";

    public MysqlDebeziumDeserialize() {
    }

    /**
     * @param sourceRecord sourceRecord
     * @param collector    out
     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) {

        try {
            Struct valueStruct = (Struct) sourceRecord.value();
            Struct afterStruct = valueStruct.getStruct("after");
            Struct beforeStruct = valueStruct.getStruct("before");
            Struct source = valueStruct.getStruct("source");
            System.out.println(afterStruct);
            /**
             *  若valueStruct中只有after,则表明插入
             *  若只有before，说明删除；
             *  若既有before，也有after，则代表更新
             */
            if (afterStruct != null && beforeStruct != null) {
                // 修改
                collector.collect(parse(beforeStruct, false));
                collector.collect(parse(afterStruct, true));
            } else if (afterStruct != null) {
                // 插入
                collector.collect(parse(afterStruct, true));
            } else if (beforeStruct != null) {
                // 删除
                System.out.println("detele");
            } else {
                System.out.println("No this operation ...");
            }
        } catch (Exception e) {
            System.out.println("Deserialize throws exception:");
        }
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.of(String.class);
    }

    private String parse(Struct valueStruct, boolean changeTime) {
        JSONObject resJson = new JSONObject();
        List<Field> fields = valueStruct.schema().fields();
        fields.forEach(item -> {
            String name = item.name();
            if (changeTime && StringUtils.equals(name, "update_time")) {
                resJson.put(name, "9999-12-31 00:00:00");
            } else {
                String type = item.schema().name();
                if (StringUtils.equals(type, "io.debezium.time.Timestamp")) {
                    Long value = (Long) valueStruct.get(name);
                    resJson.put(name, DateFormatUtils.formatUTC(value, FORMAT));
                } else {
                    resJson.put(name, valueStruct.get(name));
                }
            }
        });
        return resJson.toJSONString();
    }
}
