package flinkTest.dataStream.state_checkpoint

import bean.Sensor
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage
import org.apache.flink.streaming.api.scala._

/**
 * @Auther: wxf
 * @Date: 2022/9/19 11:41:08
 * @Description: a13_StateBackendTest  设置状态后端
 * @Version 1.0.0
 */
object A13_StateBackendTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    // 相当于 MemoryStateBackend
    // 设置 HashMapStateBackend  状态存储位置 内存
    env.setStateBackend(new HashMapStateBackend())
    // 设置 CheckPoint 存储位置 JobManager 的堆内存
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointStorage(new JobManagerCheckpointStorage())

    //    // 相当于 FsStateBackend
    //    env.setStateBackend(new HashMapStateBackend())
    //    // 设置 CheckPoint 存储位置 文件系统
    //    env.enableCheckpointing(2000)
    //    env.getCheckpointConfig.setCheckpointStorage("file:///E:\\A_data\\4.测试数据\\flink-checkPoint\\a13_StateBackendTest")
    //    //    env.getCheckpointConfig.setCheckpointStorage(new FileSystemCheckpointStorage("file:///E:\\A_data\\4.测试数据\\flink-checkPoint\\a13_StateBackendTest"))

    //    // // 相当于 RocksDBStateBackend
    //    env.setStateBackend(new EmbeddedRocksDBStateBackend(true))
    //    env.enableCheckpointing(50000)
    //    env.getCheckpointConfig.setCheckpointStorage("file:///E:\\A_data\\4.测试数据\\flink-checkPoint\\a13_StateBackendTest")


    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)
    val mapStream: DataStream[Sensor] = inputStream.map(x => {
      val str: Array[String] = x.split(" ")
      Sensor(str(0), str(1).toLong * 1000, str(2).toDouble)
    })
    // 建议 使用 KeySelector 的方式,指定key
    val keyStream: KeyedStream[Sensor, String] = mapStream.keyBy(_.id)

    val maxStream: DataStream[Sensor] = keyStream.max("temperature")
    maxStream.print("temperature：")

    env.execute("a13_StateBackendTest")

  }
}
