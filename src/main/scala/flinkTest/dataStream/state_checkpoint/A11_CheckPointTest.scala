package flinkTest.dataStream.state_checkpoint

import java.awt.image.MemoryImageSource
import java.time.Duration

import bean.Sensor
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.runtime.state.storage.{FileSystemCheckpointStorage, JobManagerCheckpointStorage}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Auther: wxf
 * @Date: 2022/9/15 20:59:10
 * @Description: a11_checkPointTest   设置 checkPoint
 * @Version 1.0.0
 */
object A11_CheckPointTest {
  def main(args: Array[String]): Unit = {

    //    val parameterTool: ParameterTool = ParameterTool.fromArgs(args)

    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    // file:///E:\A_data\flink-checkPoint\b16cefefedc16ba857dfb3d2f23dd7b2\chk-19
    conf.setString("execution.savepoint.path", args(0)) // 读取本地 checkpoint
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
//        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val checkPointPath: String = "file:///E://A_data//flink-checkPoint"

    val checkpointConfig: CheckpointConfig = env.getCheckpointConfig
    println("是否启用 检查点：" + checkpointConfig.isCheckpointingEnabled)
    println("是否启用非对齐 检查点：" + env.isUnalignedCheckpointsEnabled)
    println("是否强制启用非对齐 检查点：" + env.isForceUnalignedCheckpoints)

    // 每 1000ms 开始一次 checkpoint
    env.enableCheckpointing(1000L)
    // 设置 checkPoint 的 模式（默认 精确一次）
    checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 确认 checkpoints 之间的最小时间 500 ms  检查点之间的默认最小暂停:none。
    checkpointConfig.setMinPauseBetweenCheckpoints(500L)
    // Checkpoint 必须在一分钟内完成，否则就会被抛弃（超时时间） 尝试检查点的默认超时时间:10分钟。
    checkpointConfig.setCheckpointTimeout(60000L)
    // 允许两个连续的 checkpoint 错误  默认值为0，这意味着不容忍检查点失败，并且作业将在第一次报告检查点失败时失败。
    checkpointConfig.setTolerableCheckpointFailureNumber(2)
    // 同一时间只允许一个 checkpoint 进行   并发发生检查点的默认限制:1
    checkpointConfig.setMaxConcurrentCheckpoints(1)
    // 使用 externalized checkpoints，这样 checkpoint 在作业取消后仍就会被保留
    checkpointConfig.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // 开启实验性的 unaligned checkpoints 未对齐检查点
    checkpointConfig.enableUnalignedCheckpoints()
    // 对齐 Checkpoint 的超时设置
    checkpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(30))

    // 设置状态后端  -- 增量检查点
    env.setStateBackend(new EmbeddedRocksDBStateBackend(true))
    checkpointConfig.setCheckpointStorage(checkPointPath)


    val socketSource: DataStream[String] = env.socketTextStream("localhost", 6666)
    val mapStream: DataStream[Sensor] = socketSource.map(x => {
      val str: Array[String] = x.split(" ")
      Sensor(str(0), str(1).toLong * 1000, str(2).toDouble)
    })
    mapStream.keyBy(_.id).process(new EventCounterProcessFunction).print("mapStream：")

    env.execute("a11_checkPointTest")
  }

  case class EventCounter(id: String, var sum: Double, var count: Int)

  class EventCounterProcessFunction extends KeyedProcessFunction[String, Sensor, EventCounter] {
    private var counterState: ValueState[EventCounter] = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      // 从flink上下文中获取状态
      counterState = getRuntimeContext.getState(new ValueStateDescriptor[EventCounter]("event-counter", classOf[EventCounter]))
    }

    override def processElement(i: Sensor,
                                context: KeyedProcessFunction[String, Sensor, EventCounter]#Context,
                                collector: Collector[EventCounter]): Unit = {

      // 从状态中获取统计器，如果统计器不存在给定一个初始值
      val counter: EventCounter = Option(counterState.value()).getOrElse(EventCounter(i.id, 0.0, 0))

      // 统计聚合
      counter.count += 1
      counter.sum += i.temperature

      // 发送结果到下游
      collector.collect(counter)

      // 保存状态
      counterState.update(counter)

    }
  }

}
