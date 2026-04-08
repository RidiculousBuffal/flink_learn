package com.hpcow.sensor.source;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * Split 序列化器 - 负责将 SensorSplit 对象序列化为字节数组, 以及反向操作.

 * Flink 需要在以下场景中序列化 Split:
 *   1. JobManager 将 Split 通过网络发送给 TaskManager 上的 SourceReader.
 *   2. Checkpoint 时将 Split 的当前状态持久化到存储中.
 *   3. 从 Checkpoint 恢复时, 将字节数组反序列化回 Split 对象.

 * SimpleVersionedSerializer 是 Flink 提供的简化接口, 带有版本号支持,
 * 方便在 Split 结构发生变化时做向后兼容处理.
 */
public class SensorSplitSerializer implements SimpleVersionedSerializer<SensorSplit> {
    private static final int CURRENT_VERSION = 1;
    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(SensorSplit sensorSplit) throws IOException {
        try(ByteArrayOutputStream baos = new ByteArrayOutputStream()){
            DataOutputStream dataOutputStream = new DataOutputStream(baos);
            byte[] bytes = sensorSplit.getSensorId().getBytes(StandardCharsets.UTF_8);
            dataOutputStream.writeInt(bytes.length);
            dataOutputStream.write(bytes);
            dataOutputStream.writeDouble(sensorSplit.getBaseTemperature());
            return baos.toByteArray();
        }
    }

    @Override
    public SensorSplit deserialize(int i, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             DataInputStream dis = new DataInputStream(bais)) {
            int idLength = dis.readInt();
            byte[] idBytes = new byte[idLength];
            dis.readFully(idBytes);
            String sensorId = new String(idBytes, StandardCharsets.UTF_8);
            double baseTemperature = dis.readDouble();
            return new SensorSplit(sensorId, baseTemperature);
        }
    }
}
