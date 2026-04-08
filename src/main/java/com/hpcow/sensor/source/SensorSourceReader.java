package com.hpcow.sensor.source;


import com.hpcow.sensor.model.SensorReading;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * SourceReader - 运行在 TaskManager 上的"工人", 每个并行子任务一个实例.
 * 它的职责:
 * 1. 启动时向 SplitEnumerator 请求 Split (start 方法).
 * 2. 在 pollNext() 中根据持有的 Split 生产数据并输出给下游.
 * 3. 通过 InputStatus 返回值告知 Flink 当前的状态:
 * - MORE_AVAILABLE: 还有数据, 立刻再次调用 pollNext().
 * - NOTHING_AVAILABLE: 暂时没有数据, 等待 isAvailable() 的 Future 完成后再调用.
 * - END_OF_INPUT: 数据读取完毕, 关闭该 Reader (有界流使用).
 * 4. addSplits() 当 Enumerator 分配 Split 给该 Reader 时触发.
 * 5. snapshotState() 在 Checkpoint 时保存当前正在处理的 Split 状态.
 */
@AllArgsConstructor
@Data
public class SensorSourceReader implements SourceReader<SensorReading, SensorSplit> {


    private final SourceReaderContext context;
    private final Queue<SensorSplit> assignedSplits = new ArrayDeque<>();
    private SensorSplit currentSplit = null;
    private final Random random = new Random();
    private long lastEmitTime = 0;
    private static final long EMIT_INTERVAL_MS = 500;
    private CompletableFuture<Void> availability = new CompletableFuture<>();


    public SensorSourceReader(SourceReaderContext context) {
        this.context = context;
    }

    @Override
    public void start() {
        context.sendSplitRequest();
    }


    private void scheduleNextEmit() {
        // 重置 availability, 确保它是未完成状态
        if (availability.isDone()) {
            availability = new CompletableFuture<>();
        }
        long delay = EMIT_INTERVAL_MS - (System.currentTimeMillis() - lastEmitTime);
        if (delay <= 0) {
            availability.complete(null);
            return;
        }
        // 在 delay 毫秒后完成 Future, 通知 Flink 可以再次调用 pollNext()
        final CompletableFuture<Void> currentAvailability = availability;
        Thread wakeupThread = new Thread(() -> {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            currentAvailability.complete(null);
        });
        wakeupThread.setDaemon(true);
        wakeupThread.start();
        return;
    }

    @Override
    public InputStatus pollNext(ReaderOutput<SensorReading> readerOutput) throws Exception {
        if (currentSplit == null) {
            currentSplit = assignedSplits.poll();
        }

        if (currentSplit == null) {
            return InputStatus.NOTHING_AVAILABLE;
        }

        long now = System.currentTimeMillis();
        if (now - lastEmitTime < EMIT_INTERVAL_MS) {
            scheduleNextEmit();
            return InputStatus.NOTHING_AVAILABLE;
        }

        double temperature = currentSplit.getBaseTemperature() + (random.nextDouble() - 0.5) * 10.0;
        SensorReading reading = new SensorReading(currentSplit.getSensorId(), now, temperature);
        readerOutput.collect(reading);
        lastEmitTime = now;
        return InputStatus.MORE_AVAILABLE;

    }

    @Override
    public List<SensorSplit> snapshotState(long l) {
        List<SensorSplit> state = new ArrayList<>();
        if (currentSplit != null) {
            state.add(currentSplit);
        }
        state.addAll(assignedSplits);
        return state;
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return availability;
    }

    @Override
    public void addSplits(List<SensorSplit> list) {
        assignedSplits.addAll(list);
        System.out.printf("[Reader subtask=%d] Received splits:%s%n", context.getIndexOfSubtask(), list);
        availability.complete(null);
    }

    @Override
    public void notifyNoMoreSplits() {
        System.out.printf("[Reader subtask=%d] No more splits will be assigned. %n", context.getIndexOfSubtask());

    }

    @Override
    public void close() throws Exception {

    }
}
