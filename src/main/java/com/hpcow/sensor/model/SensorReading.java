package com.hpcow.sensor.model;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class SensorReading {
    public String sensorId;
    public long timeStamp;
    public double temperature;
}
