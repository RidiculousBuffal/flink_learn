package com.hpcow.sensor.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class TemperatureAlert {
    public String sensorId;
    public double temperature;
    public String reason;
    public long alertTime;
}
