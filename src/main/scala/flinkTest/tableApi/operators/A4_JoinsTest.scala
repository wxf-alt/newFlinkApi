package flinkTest.tableApi.operators

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.{DataTypes, Table, row}
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

/**
 * @Auther: wxf
 * @Date: 2022/10/24 16:50:28
 * @Description: A4_JoinsTest
 * @Version 1.0.0
 */
object A4_JoinsTest {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 环境配置
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val inputStream1: DataStream[(String, String, String)] = env.socketTextStream("localhost", 6666)
      .map(x => {
        val str: Array[String] = x.split(" ")
        (str(0), str(1), str(2))
      })
    val inputStream2: DataStream[(String, String, String)] = env.socketTextStream("localhost", 7777)
      .map(x => {
        val str: Array[String] = x.split(" ")
        (str(0), str(1), str(2))
      })
    val left: Table = tableEnv.fromDataStream(inputStream1).as("a", "b", "c")
    val right: Table = tableEnv.fromDataStream(inputStream2).as("d", "e", "f")


    //    // inner join 内连接
    left.join(right, $"a" === $"d")
      .select($"a", $"b", $"c", $"e", $"f")
      .toAppendStream[Row]
      .print()

    // Outer Join 外连接
    left.leftOuterJoin(right, $"a" === $"d").select($"a", $"b", $"e").toRetractStream[Row].filter(_._1).print()
    left.rightOuterJoin(right, $"a" === $"d").select($"a", $"b", $"e").toRetractStream[Row].filter(_._1).print()

    // Full Join 全连接
    left.fullOuterJoin(right, $"a" === $"d").select($"a", $"b", $"e").toRetractStream[Row].filter(_._1).print()


    // joinLateral join 表和表函数的结果
    val mySplitUDTF: MySplitUDTF = new MySplitUDTF
    left.joinLateral(mySplitUDTF($"c") as("s", "t"))
      .select($"a", $"b", $"s", $"t")
      .toRetractStream[Row].filter(_._1).print()

    env.execute("A4_JoinsTest")
  }

  class MySplitUDTF extends TableFunction[(String, Int)] {
    def eval(str: String) = {
      val array: Array[String] = str.split(",")
      array.foreach(
        word => collect(word, word.length)
      )
    }
  }

}