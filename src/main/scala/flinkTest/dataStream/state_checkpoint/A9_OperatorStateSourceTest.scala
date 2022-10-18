package flinkTest.dataStream.state_checkpoint

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._

/**
 * @Auther: wxf
 * @Date: 2022/9/15 16:40:27
 * @Description: a9_operatorStateSourceTest   带状态的 Source Function
 * @Version 1.0.0
 */
object a9_OperatorStateSourceTest extends App {

  val conf: Configuration = new Configuration()
  conf.setInteger(RestOptions.PORT, 8081)
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
  env.setParallelism(2)

  env.setStateBackend(new FsStateBackend("file:///E:\\A_data\\4.测试数据\\flink-checkPoint\\a9_operatorStateSourceTest", true))
  env.enableCheckpointing(5000)

  private val sourceStream: DataStream[Long] = env.addSource(new CounterSource)
  sourceStream.print("sourceStream：")

  env.execute("a9_operatorStateSourceTest")
}

class CounterSource extends RichParallelSourceFunction[Long] with CheckpointedFunction {

  @volatile
  private var isRunning: Boolean = true
  private var offset: Long = 0L
  private var state: ListState[Long] = _

  // 快照
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    state.clear()
    state.add(offset)
  }

  // 初始化
  override def initializeState(context: FunctionInitializationContext): Unit = {
    // 创建/或者恢复状态
    state = context.getOperatorStateStore.getListState(new ListStateDescriptor("state", TypeInformation.of(classOf[Long])))
    //    import scala.collection.JavaConverters._
    import scala.collection.convert.ImplicitConversions._
    for (elem <- state.get()) {
      offset = elem
    }
  }

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    val lock: AnyRef = ctx.getCheckpointLock
    while (isRunning) {
      // 输出和状态更新是原子的
      lock.synchronized({
        //        Thread.sleep(500)
        ctx.collect(offset)
        offset += 1
      })
    }
  }

  override def cancel(): Unit = isRunning = false

}

