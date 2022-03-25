package com.cn.flink.source;

import com.cn.flink.domain.SensorData;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 自定义Souce
 * 如果是是实现SourceFunction接口，则并行度只能设为1，否则会抛异常
 *
 * @author Chen Nan
 */
public class MySourceFunction implements ParallelSourceFunction<SensorData> {
    // 自定义停止标识
    private boolean stop = false;

    @Override
    public void run(SourceContext<SensorData> ctx) throws Exception {
        SensorData sensorData = new SensorData();
        while (!stop) {
            long id = RandomUtils.nextLong(1, 10);
            double value = RandomUtils.nextDouble(1, 100);
            sensorData.setId(id);
            sensorData.setName("sensor" + id);
            sensorData.setValue(value);
            sensorData.setTimestamp(System.currentTimeMillis());
            ctx.collect(sensorData);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        stop = true;
    }
}
