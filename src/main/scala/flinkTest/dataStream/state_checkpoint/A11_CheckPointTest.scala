package flinkTest.dataStream.state_checkpoint

import java.time.Duration

import bean.Sensor
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala._

/**
 * @Auther: wxf
 * @Date: 2022/9/15 20:59:10
 * @Description: a11_checkPointTest   设置 checkPoint
 * @Version 1.0.0
 */
object A11_CheckPointTest {
  def main(args: Array[String]): Unit = {

    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    println("是否启用 检查点：" + env.getCheckpointConfig.isCheckpointingEnabled)
    println("是否启用非对齐 检查点：" + env.isUnalignedCheckpointsEnabled)
    println("是否强制启用非对齐 检查点：" + env.isForceUnalignedCheckpoints)

    // 每 1000ms 开始一次 checkpoint
    env.enableCheckpointing(1000L)
    // 设置 checkPoint 的 模式（默认 精确一次）
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 确认 checkpoints 之间的最小时间 500 ms  检查点之间的默认最小暂停:none。
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)
    // Checkpoint 必须在一分钟内完成，否则就会被抛弃（超时时间） 尝试检查点的默认超时时间:10分钟。
    env.getCheckpointConfig.setCheckpointTimeout(60000L)
    // 允许两个连续的 checkpoint 错误  默认值为0，这意味着不容忍检查点失败，并且作业将在第一次报告检查点失败时失败。
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(2)
    // 同一时间只允许一个 checkpoint 进行   并发发生检查点的默认限制:1
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 使用 externalized checkpoints，这样 checkpoint 在作业取消后仍就会被保留
    env.getCheckpointConfig.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // 开启实验性的 unaligned checkpoints 未对齐检查点
    env.getCheckpointConfig.enableUnalignedCheckpoints()
    // 对齐 Checkpoint 的超时设置
    env.getCheckpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(30))

    val socketSource: DataStream[String] = env.socketTextStream("localhost", 6666)
    val mapStream: DataStream[Sensor] = socketSource.map(x => {
      val str: Array[String] = x.split(" ")
      Sensor(str(0), str(1).toLong * 1000, str(2).toDouble)
    })
    mapStream.print("mapStream：")

    env.execute("a11_checkPointTest")
  }
}
