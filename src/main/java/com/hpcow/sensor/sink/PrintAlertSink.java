package com.hpcow.sensor.sink;

import com.hpcow.sensor.model.TemperatureAlert;
import org.apache.flink.api.connector.sink2.InitContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import java.io.IOException;

/**
 * PrintAlertSink - 自定义 Sink 的顶层工厂类 (新 Sink API, FLIP-191/FLIP-372).
 *
 * 在新 Sink API 中, Sink 接口本身只是一个"工厂", 职责非常单一:
 * 实现 createWriter() 方法, 为每个并行子任务创建一个 SinkWriter 实例.
 * 实际的写出逻辑全部在 PrintAlertSinkWriter 中.
 *
 * Sink 接口的泛型参数 T 是输入数据的类型, 这里是 TemperatureAlert.
 *
 * Sink 实例本身会被序列化后发送到集群, 因此必须是可序列化的.
 * (实现 java.io.Serializable 或只包含可序列化的字段)
 *
 * 注册方式:
 *   旧 API: stream.addSink(new PrintAlertSink())
 *   新 API: stream.sinkTo(new PrintAlertSink())
 */
public class PrintAlertSink implements Sink<TemperatureAlert> {

    @Override
    public SinkWriter<TemperatureAlert> createWriter(WriterInitContext writerInitContext) throws IOException {
        System.out.println(writerInitContext.getTaskInfo().getIndexOfThisSubtask());
        return new PrintAlertSinkWriter(writerInitContext.getTaskInfo().getIndexOfThisSubtask());
    }
}

