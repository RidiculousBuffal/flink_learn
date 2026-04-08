package com.hpcow.sensor.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.flink.api.connector.source.SourceSplit;

@AllArgsConstructor
@ToString
@Data
public class SensorSplit implements SourceSplit {
    private final String sensorId;
    private final double baseTemperature;
    @Override
    public String splitId() {
        return sensorId;
    }

}
