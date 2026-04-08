package com.hpcow.sensor.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

@Data
@AllArgsConstructor
public class SensorSplitEnumerator implements SplitEnumerator<SensorSplit, List<SensorSplit>> {

    private final SplitEnumeratorContext<SensorSplit> context;
    private final Queue<SensorSplit> pendingSplits;
    private final int numSensors;

    public SensorSplitEnumerator(SplitEnumeratorContext<SensorSplit> context, int numSensors) {
        this.context = context;
        this.numSensors = numSensors;
        this.pendingSplits = new ArrayDeque<>();
    }




    @Override
    public void start() {
        if(pendingSplits.isEmpty()){
            Random random = new Random();
            for(int i = 0 ; i < numSensors ; i++){
                String sensorId = String.format("sensor-%03d",i+1);
                double baseTemp = 15.0 + random.nextDouble()*20;
                pendingSplits.offer(new SensorSplit(sensorId,baseTemp));
            }
        }

    }

    @Override
    public void handleSplitRequest(int subTaskId, @Nullable String requesterHostname) {
        SensorSplit split = pendingSplits.poll();
        if(split!=null){
            context.assignSplit(split,subTaskId);
            System.out.printf("[Enumerator] Assigned %s to subtask %d%n",split.getSensorId(),subTaskId);
        }

    }

    //失败的时候
    @Override
    public void addSplitsBack(List<SensorSplit> list, int subtaskId) {
        System.out.printf("[Enumerator] Subtask %d failed,taking back %d splits %n",subtaskId,list.size());
        pendingSplits.addAll(list);
    }

    @Override
    public void addReader(int subTaskId) {
        SensorSplit split = pendingSplits.poll();
        if(split!=null){
            context.assignSplit(split,subTaskId);
            System.out.printf("[Enumerator] Pushed %s to new subtask %d%n",split.getSensorId(),subTaskId);
        }

    }

    @Override
    public List<SensorSplit> snapshotState(long checkpointId) throws Exception {
        return new ArrayList<>(pendingSplits);
    }

    @Override
    public void close() throws IOException {
        // 无需释放
    }
}
