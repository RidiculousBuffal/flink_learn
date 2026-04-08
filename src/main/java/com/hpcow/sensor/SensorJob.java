package com.hpcow.sensor;

import com.hpcow.sensor.model.SensorReading;
import com.hpcow.sensor.model.TemperatureAlert;
import com.hpcow.sensor.process.TemperatureAlertFunction;
import com.hpcow.sensor.sink.PrintAlertSink;
import com.hpcow.sensor.source.SensorSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SensorJob {
    private static final int PARALLELISM = 3;

    public static void main(String[] args) throws Exception {

        // =====================================================================
        // 【模式 A】本地运行 / 【模式 C】打包提交 - 通用写法
        // =====================================================================
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        env.setParallelism(PARALLELISM);

        // =====================================================================
        // Step 1: 使用新 Source API 添加自定义数据源
        //
        // 新 API 使用 env.fromSource() 而不是旧的 env.addSource().
        //
        // fromSource() 的三个参数:
        //   1. source:            自定义 Source 实例
        //   2. WatermarkStrategy: 水印策略, 用于事件时间处理.
        //                         noWatermarks() 表示不使用事件时间(使用处理时间).
        //   3. sourceName:        Source 算子的名称, 显示在 Flink Web UI 上.
        // =====================================================================
        DataStream<SensorReading> readings = env
                .fromSource(
                        new SensorSource(PARALLELISM),
                        WatermarkStrategy.noWatermarks(),
                        "sensor-source"
                );

        // =====================================================================
        // Step 2: filter 过滤无效数据
        // =====================================================================
        DataStream<SensorReading> validReadings = readings
                .filter(r -> r.temperature > -273.15)
                .name("filter-invalid");

        // =====================================================================
        // Step 3: keyBy 分区 + process 带状态处理
        // keyBy 保证相同 sensorId 的数据路由到同一个 process 实例
        // =====================================================================
        DataStream<TemperatureAlert> alerts = validReadings
                .keyBy(r -> r.sensorId)
                .process(new TemperatureAlertFunction())
                .name("temperature-alert-detector");

        // =====================================================================
        // Step 4: 自定义 Sink 输出
        //
        // 新 API 使用 .sinkTo() 而不是旧的 .addSink().
        // sinkTo() 返回 DataStreamSink, 可以继续调用 .name() 等方法.
        // =====================================================================
        alerts
                .rebalance()
                .sinkTo(new PrintAlertSink())
                .name("print-alert-sink")
                .setParallelism(3);

        // =====================================================================
        // Step 5: 触发执行
        // =====================================================================
        env.execute("Sensor Temperature Alert Job");
    }
}
