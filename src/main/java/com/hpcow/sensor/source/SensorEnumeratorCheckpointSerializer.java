package com.hpcow.sensor.source;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class SensorEnumeratorCheckpointSerializer
        implements SimpleVersionedSerializer<List<SensorSplit>> {

    private static final int CURRENT_VERSION = 1;
    private final SensorSplitSerializer splitSerializer;

    public SensorEnumeratorCheckpointSerializer(SensorSplitSerializer splitSerializer) {
        this.splitSerializer = splitSerializer;
    }

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(List<SensorSplit> splits) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeInt(splits.size());
            for (SensorSplit split : splits) {
                byte[] splitBytes = splitSerializer.serialize(split);
                dos.writeInt(splitBytes.length);
                dos.write(splitBytes);
            }
            return baos.toByteArray();
        }
    }

    @Override
    public List<SensorSplit> deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             DataInputStream dis = new DataInputStream(bais)) {
            int size = dis.readInt();
            List<SensorSplit> splits = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                int splitLength = dis.readInt();
                byte[] splitBytes = new byte[splitLength];
                dis.readFully(splitBytes);
                splits.add(splitSerializer.deserialize(splitSerializer.getVersion(), splitBytes));
            }
            return splits;
        }
    }
}
