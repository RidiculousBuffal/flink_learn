package com.hpcow.sensor.process;

import com.hpcow.sensor.model.SensorReading;
import com.hpcow.sensor.model.TemperatureAlert;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class TemperatureAlertFunction extends KeyedProcessFunction<String, SensorReading, TemperatureAlert> {

    private static final double HIGH_TEMP_THRESHOLD = 40.0;
    private static final double RISE_THRESHOLD = 8.0;

    /**
     * ValueState 用于存储每个传感器上一次的温度值.
     *
     * transient 关键字: 告诉 Java 序列化机制不要序列化这个字段.
     * 因为 ValueState 是由 Flink 运行时管理的, 不能被普通 Java 序列化.
     * 它的实际初始化在 open() 方法中通过 getRuntimeContext().getState() 完成.
     */
    private transient ValueState<Double> lastTemperatureState;

    /**
     * open() 在算子初始化时调用一次.
     * 所有 State 必须在这里通过 StateDescriptor 注册和获取.
     *
     * ValueStateDescriptor 的两个参数:
     *   1. 状态名称: 在同一个算子内必须唯一, 用于区分不同的状态变量.
     *   2. 状态值的类型信息: Flink 用它来序列化/反序列化状态数据.
     */



    @Override
    public void open(OpenContext context) throws Exception {
        ValueStateDescriptor<Double> descriptor = new ValueStateDescriptor<>(
                "last-temperature",  // 状态名称
                Types.DOUBLE         // 值类型
        );
        lastTemperatureState = getRuntimeContext().getState(descriptor);
    }

    /**
     * processElement() 对每一条输入数据调用一次.
     *
     * @param reading  当前输入的传感器读数
     * @param ctx      上下文, 可以访问当前 Key、时间戳、注册定时器等
     * @param out      输出收集器, 调用 out.collect() 向下游发送数据
     */
    @Override
    public void processElement(SensorReading reading, Context ctx, Collector<TemperatureAlert> out)
            throws Exception {

        // 1. 超温检测: 直接判断当前温度
        if (reading.temperature > HIGH_TEMP_THRESHOLD) {
            out.collect(new TemperatureAlert(
                    reading.sensorId,
                    reading.temperature,
                    String.format("温度超过阈值 %.1f°C (当前: %.2f°C)", HIGH_TEMP_THRESHOLD, reading.temperature),
                    reading.timeStamp
            ));
        }

        // 2. 持续升温检测: 需要与上一次的温度比较
        // state.value() 返回当前 Key 对应的状态值, 如果从未设置过则返回 null
        Double lastTemperature = lastTemperatureState.value();

        if (lastTemperature != null) {
            double rise = reading.temperature - lastTemperature;
            if (rise > RISE_THRESHOLD) {
                out.collect(new TemperatureAlert(
                        reading.sensorId,
                        reading.temperature,
                        String.format("温度急剧上升 +%.2f°C (%.2f -> %.2f)", rise, lastTemperature, reading.temperature),
                        reading.timeStamp
                ));
            }
        }

        // 3. 更新状态: 将当前温度保存为"上一次温度", 供下一条数据使用
        // state.update() 会覆盖当前 Key 的状态值
        lastTemperatureState.update(reading.temperature);
    }
}
