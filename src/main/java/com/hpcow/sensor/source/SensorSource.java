package com.hpcow.sensor.source;
import com.hpcow.sensor.model.SensorReading;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * SensorSource - 自定义数据源的顶层入口类 (新 Source API, FLIP-27).
 *
 * 这个类本身只是一个"工厂", 负责:
 *   1. 声明数据源的有界性 (Boundedness): CONTINUOUS_UNBOUNDED 表示无界流.
 *   2. 创建 SourceReader 实例 (运行在 TaskManager 上).
 *   3. 创建 SplitEnumerator 实例 (运行在 JobManager 上).
 *   4. 提供 Split 和 EnumeratorCheckpoint 的序列化器.
 *
 * Source 接口的三个泛型参数:
 *   - T:        输出的数据类型, 即 SensorReading
 *   - SplitT:   Split 的类型, 即 SensorSplit
 *   - EnumChkT: Enumerator Checkpoint 的类型, 即 List<SensorSplit>
 *
 * Source 实例本身会被序列化后发送到集群, 所以必须是可序列化的.
 *
 * 并行度说明:
 *   新 Source API 天然支持并行. 并行度由 env.setParallelism() 或
 *   算子级别的 .setParallelism() 控制. Flink 会为每个并行子任务
 *   创建一个独立的 SourceReader 实例.
 */
public class SensorSource implements Source<SensorReading, SensorSplit, List<SensorSplit>> {

    /**
     * 传感器数量, 决定了 SplitEnumerator 会生成多少个 Split.
     * 建议将此值设置为与 Source 的并行度相同, 以便每个 SourceReader 分配到一个 Split.
     */
    private final int numSensors;

    public SensorSource(int numSensors) {
        this.numSensors = numSensors;
    }

    /**
     * getBoundedness() 声明数据源的有界性.
     *   CONTINUOUS_UNBOUNDED: 无界流(流处理模式), Source 会一直运行.
     *   BOUNDED:              有界流(批处理模式), Source 读完数据后会结束.
     */
    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    /**
     * createReader() 为每个并行子任务创建一个 SourceReader 实例.
     * 此方法在 TaskManager 上调用.
     */
    @Override
    public SourceReader<SensorReading, SensorSplit> createReader(SourceReaderContext readerContext) {
        return new SensorSourceReader(readerContext);
    }

    /**
     * createEnumerator() 在 Job 首次启动时创建 SplitEnumerator.
     * 此方法在 JobManager 上调用.
     */
    @Override
    public SplitEnumerator<SensorSplit, List<SensorSplit>> createEnumerator(
            SplitEnumeratorContext<SensorSplit> enumContext) {
        return new SensorSplitEnumerator(enumContext, numSensors);
    }

    /**
     * restoreEnumerator() 在从 Checkpoint 恢复时创建 SplitEnumerator.
     * checkpoint 参数是上次 snapshotState() 保存的状态.
     */
    @Override
    public SplitEnumerator<SensorSplit, List<SensorSplit>> restoreEnumerator(
            SplitEnumeratorContext<SensorSplit> enumContext,
            List<SensorSplit> checkpoint) {
        return new SensorSplitEnumerator(enumContext, (Queue<SensorSplit>) checkpoint, numSensors);
    }

    /**
     * getSplitSerializer() 返回 Split 的序列化器.
     * Flink 用它来在 JobManager 和 TaskManager 之间传输 Split 对象.
     */
    @Override
    public SimpleVersionedSerializer<SensorSplit> getSplitSerializer() {
        return new SensorSplitSerializer();
    }

    /**
     * getEnumeratorCheckpointSerializer() 返回 Enumerator Checkpoint 的序列化器.
     * Flink 用它来持久化 SplitEnumerator 的状态 (即 List<SensorSplit>).
     */
    @Override
    public SimpleVersionedSerializer<List<SensorSplit>> getEnumeratorCheckpointSerializer() {
        return new SensorEnumeratorCheckpointSerializer(new SensorSplitSerializer());
    }
}
