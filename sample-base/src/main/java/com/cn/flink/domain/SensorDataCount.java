package com.cn.flink.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.time.DateFormatUtils;

/**
 * @author Chen Nan
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SensorDataCount implements Comparable<SensorDataCount> {
    private Long id;
    private Integer count;
    private Long windowStart;
    private Long windowEnd;

    @Override
    public String toString() {
        return "SensorDataCount{" +
                "id=" + id +
                ", count=" + count +
                ", windowStart=" + DateFormatUtils.format(windowStart, DateFormatUtils.ISO_DATETIME_FORMAT.getPattern()) +
                ", windowEnd=" + DateFormatUtils.format(windowEnd, DateFormatUtils.ISO_DATETIME_FORMAT.getPattern()) +
                '}';
    }

    @Override
    public int compareTo(SensorDataCount o) {
        return o.getCount().compareTo(this.getCount());
    }
}
