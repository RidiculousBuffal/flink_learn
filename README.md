# Flink DataStream 2.0+ 开发 CheatSheet

这份备忘录为你梳理了开发一个标准 Flink 任务的**整体顺序**，以及**各个组件（Source、Process、Sink）必须实现的方法和编写顺序**。

---

## 1. 定义一个 Flink Jar 的整体顺序

开发一个 Flink 数据流处理作业，通常遵循以下标准步骤：

1. **定义数据模型 (POJO)**：编写表示输入数据和输出结果的 Java 类（如 `SensorReading`、`TemperatureAlert`）。建议使用原生的 public 字段或完整的 getter/setter，以获得最佳的序列化性能。
2. **编写自定义 Source**：如果需要从自定义外部系统读取数据，实现 FLIP-27 Source API。
3. **编写核心处理逻辑**：实现 `ProcessFunction` 或 `KeyedProcessFunction`，这是业务逻辑和状态管理的核心。
4. **编写自定义 Sink**：如果需要写出到自定义外部系统，实现 FLIP-191 Sink API。
5. **组装 Job DAG**：在主类（如 `SensorJob.java`）的 `main` 方法中，使用 `StreamExecutionEnvironment` 将上述组件串联起来。
6. **配置与打包**：在 `pom.xml` 中配置依赖（注意 `provided` 作用域），使用 `mvn clean package` 打包成 Fat Jar。

---

## 2. 编写 Source 的顺序与必备组件 (FLIP-27 API)

新版 Source API 包含 5 个核心组件，建议按以下顺序编写：

### 步骤 1：定义工作单元 `Split`
- **必须实现**：`SourceSplit` 接口。
- **核心方法**：`splitId()` 返回唯一标识符。
- **作用**：描述“读取哪一部分数据”，比如一个文件路径、一个 Kafka Partition，或者一个起始温度。

### 步骤 2：编写 `SplitSerializer`（关于你的疑问：可以省掉吗？）
**结论：在大多数情况下，是的，你可以省掉手写序列化器！**

Flink 提供了 `SimpleVersionedSerializerAdapter` 结合 Java 原生序列化（或 Flink 自己的 TypeInformation）来简化这一步。如果你的 `Split` 类实现了 `java.io.Serializable`，你可以这样直接创建一个内置的序列化器：

```java
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.io.SimpleVersionedSerializerAdapter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

// 使用 Flink 内置的适配器，免去手写字节流的痛苦
SimpleVersionedSerializer<SensorSplit> splitSerializer = 
    new SimpleVersionedSerializerAdapter<>(TypeInformation.of(SensorSplit.class).createSerializer(null));
```
*注：手写 `DataOutputStream` 的方式（如之前的示例）在极致性能优化时使用，对于普通业务完全可以用内置适配器替代。*

### 步骤 3：编写 `SplitEnumerator` (大脑)
- **必须实现**：`SplitEnumerator<SplitT, EnumStateT>` 接口。
- **核心方法**：
    - `start()`：启动枚举器（可在此启动定时发现新 Split 的线程）。
    - `handleSplitRequest()`：当 Reader 请求任务时，分配 Split 给它。
    - `addSplitsBack()`：当 Reader 失败时，回收它未完成的 Split。
    - `snapshotState()`：Checkpoint 时保存 Enumerator 的状态（比如已经发现了哪些文件）。

### 步骤 4：编写 `SourceReader` (工人)
- **必须实现**：`SourceReader<T, SplitT>` 接口。
- **核心方法**：
    - `start()`：启动 Reader。
    - `pollNext(ReaderOutput)`：**最核心的方法**，从外部系统拉取数据并调用 `output.collect()` 发送给下游。
    - `addSplits()`：接收来自 Enumerator 分配的新 Split。
    - `snapshotState()`：Checkpoint 时保存读取进度（如 Kafka Offset）。

### 步骤 5：组装 `Source` (工厂)
- **必须实现**：`Source<T, SplitT, EnumStateT>` 接口。
- **核心方法**：
    - `createReader()`：实例化 `SourceReader`。
    - `createEnumerator()`：实例化 `SplitEnumerator`。
    - `getSplitSerializer()`：返回 Split 序列化器。

---

## 3. 编写 Process 的顺序与必备组件

如果你需要使用状态（State）或定时器（Timer），必须使用 `KeyedProcessFunction`。建议编写顺序如下：

### 步骤 1：定义并初始化状态
- **必须重写**：`open(OpenContext context)` 方法。
- **作用**：在这里创建 `StateDescriptor` 并通过 `getRuntimeContext().getState(...)` 获取状态句柄。
- **注意**：状态变量必须声明为 `transient`。

### 步骤 2：处理每一条数据
- **必须重写**：`processElement(I value, Context ctx, Collector<O> out)` 方法。
- **作用**：
    - 读取当前状态：`state.value()`
    - 执行业务逻辑（计算、比较等）
    - 输出结果：`out.collect(...)`
    - 更新状态：`state.update(...)`
    - （可选）注册定时器：`ctx.timerService().registerEventTimeTimer(...)`

### 步骤 3：处理定时器触发（可选）
- **必须重写**：`onTimer(long timestamp, OnTimerContext ctx, Collector<O> out)` 方法。
- **作用**：当之前注册的时间到达时，执行清理状态或输出迟到数据等逻辑。

---

## 4. 编写 Sink 的顺序与必备组件 (FLIP-191 API)

新版 Sink API 拆分了工厂和执行器，编写非常简单：

### 步骤 1：编写 `SinkWriter` (执行器)
- **必须实现**：`SinkWriter<InputT>` 接口。
- **核心方法**：
    - **构造函数**：在这里执行初始化（建立数据库连接、打开文件等）。**注意：不要使用 Lombok 的 `@AllArgsConstructor`，以免引起字段初始化冲突。**
    - `write(InputT element, Context context)`：对每一条到达的数据执行写出操作。
    - `flush(boolean endOfInput)`：在 Checkpoint 触发时调用，用于将缓冲区数据强制刷写到外部系统（如 `connection.commit()`）。
    - `close()`：释放连接和资源。

### 步骤 2：编写 `Sink` (工厂)
- **必须实现**：`Sink<InputT>` 接口。
- **核心方法**：
    - `createWriter(WriterInitContext context)`：实例化并返回你刚刚写的 `SinkWriter`。可以从 `context` 中获取当前 SubTask 的编号。

---

## 5. 打包与提交命令速查

### Maven 打包原则
在 `pom.xml` 中，Flink 核心依赖（如 `flink-streaming-java`, `flink-clients`）的 `<scope>` 必须设置为 `provided`。自定义的第三方依赖（如 MySQL 驱动、JSON 解析库）保留默认的 `compile` 作用域。

打包命令：
```bash
mvn clean package
```
生成的 Jar 包通常位于 `target/your-project-1.0-SNAPSHOT.jar`。

### 远程提交命令 (CLI)
```bash
# 提交到指定的 JobManager (例如 localhost:8081)
# -d: 后台分离运行 (detached)
# -c: 指定主类名
./bin/flink run -m localhost:8081 -d -c com.example.SensorJob /path/to/your-project.jar
```
