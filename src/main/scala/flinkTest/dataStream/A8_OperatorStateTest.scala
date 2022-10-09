package flinkTest.dataStream

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment._
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

import scala.collection.mutable.ListBuffer
import scala.collection.convert.wrapAll._

/**
 * @Auther: wxf
 * @Date: 2022/9/15 14:49:17
 * @Description: a8_operatorStateTest   算子状态测试
 * @Version 1.0.0
 */
object a8_OperatorStateTest extends App {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)


  private val list: List[(String, Long)] = List(("aa", 3L), ("bb", 5L), ("cc", 7L), ("dd", 4L), ("ee", 2L))
  val sourceStream: DataStreamSource[(String, Long)] = env.fromCollection(list)
  sourceStream.addSink(new BufferingSink)

  env.execute("a8_operatorStateTest")
}

class BufferingSink(threshold: Int = 0) extends RichSinkFunction[(String, Long)] with CheckpointedFunction {

  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  private val bufferedElements: ListBuffer[(String, Long)] = ListBuffer[(String, Long)]()
  @transient
  private var checkpointedState: ListState[(String, Long)] = _

  // 创建数据库连接
  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/db", "root", "root")
    insertStmt = conn.prepareStatement("insert into operatorState (id, temp) values (?,?)")
    updateStmt = conn.prepareStatement("update operatorState set temp = ? where id = ?")
  }

  override def invoke(value: (String, Long), context: SinkFunction.Context): Unit = {
    bufferedElements += value
    if (bufferedElements.size > threshold) {
      for (elem <- bufferedElements) {
        // 执行更新语句
        updateStmt.setLong(1, elem._2)
        updateStmt.setString(2, elem._1)
        updateStmt.execute
        if (updateStmt.getUpdateCount == 0) {
          insertStmt.setString(1, elem._1)
          insertStmt.setLong(2, elem._2)
          insertStmt.execute()
        }
        bufferedElements.clear()
      }
    }
  }

  // 关闭操作
  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }


  // 进行 checkpoint 时会调用 snapshotState()
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    checkpointedState.clear()
    for (elem <- bufferedElements) {
      checkpointedState.add(elem)
    }
  }

  // 用户自定义函数初始化时会调用 initializeState()
  override def initializeState(context: FunctionInitializationContext): Unit = {
    val descriptor: ListStateDescriptor[(String, Long)] = new ListStateDescriptor[(String, Long)](
      "buffered-elements", TypeInformation.of(new TypeHint[(String, Long)]() {})
    )
    // 使用 union redistribution 算法
    //    val checkpointedState: ListState[(String, Long)] = context.getOperatorStateStore.getUnionListState(descriptor)
    // 使用 even-split redistribution 算法
    val checkpointedState: ListState[(String, Long)] = context.getOperatorStateStore.getListState(descriptor)
    if (context.isRestored) {
      for (element <- checkpointedState.get()) {
        bufferedElements += element
      }
    }
  }

}
