package com.cn.flink.source;

import com.cn.flink.domain.SensorData;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 自定义Souce
 *
 * @author Chen Nan
 */
public class MySourceFunction implements SourceFunction<SensorData> {
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
            ctx.collect(sensorData);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        stop = true;
    }
}
