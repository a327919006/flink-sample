package com.cn.flink.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义数据源
 * <p>
 *
 * @author Chen Nan
 */
public class SourceTest4_Customer {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new MySourceFunction())
                .print();

        env.execute();
    }
}
