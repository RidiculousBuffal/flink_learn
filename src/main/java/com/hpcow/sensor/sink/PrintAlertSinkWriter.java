package com.hpcow.sensor.sink;

import com.hpcow.sensor.model.TemperatureAlert;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

@Data
public class PrintAlertSinkWriter implements SinkWriter<TemperatureAlert> {
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final int subtaskIndex;

    private long alertCount = 0;

    public PrintAlertSinkWriter(int subtaskIndex) {
        System.out.println(subtaskIndex);
        this.subtaskIndex = subtaskIndex;
    }

    @Override
    public void write(TemperatureAlert temperatureAlert, Context context) throws IOException, InterruptedException {
        alertCount++;
        String time = LocalDateTime.ofInstant(Instant.ofEpochMilli(temperatureAlert.alertTime), ZoneId.systemDefault()).format(FORMATTER);

        System.out.printf("[subtask-%d] >>> [%s] #%d %s%n",subtaskIndex,time,alertCount,temperatureAlert);
    }

    public void flush(boolean endOfInput) {

    }

    public void close(){
        System.out.printf("[PrintAlertSinkWriter] Subtask %d closed. Total alerts: %d %n",subtaskIndex,alertCount);
    }

}
