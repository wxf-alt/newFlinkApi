package flinkTest.tableApi.custom_function

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
 * @Auther: wxf
 * @Date: 2022/11/2 15:10:34
 * @Description: Top2TableAggregateFunction   一共有 5 行。假设你需要找到价格最高的两个饮料
 * @Version 1.0.0
 */
object Top2TableAggregateFunction {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val table: Table = tableEnv.fromValues(DataTypes.ROW(
      DataTypes.FIELD("id", DataTypes.INT()),
      DataTypes.FIELD("name", DataTypes.STRING()),
      DataTypes.FIELD("price", DataTypes.INT())),
      row(1, "Latte", 6),
      row(1, "Tea", 4),
      row(1, "Teaqw", 9),
      row(1, "Te21a", 6),
      row(1, "Teafre", 1)
    )

    val top2: Top2 = new Top2
    table.groupBy($"id")
      .flatAggregate(top2($"name", $"price") as("name", "price", "rank"))
      .select($"id", $"name", $"price", $"rank")
      .toRetractStream[Row]
      .filter(_._1)
      .print("table")

    env.execute("Top2TableAggregateFunction")
  }

  case class Top2Accum(var first: (String, Int), var second: (String, Int))


  class Top2 extends TableAggregateFunction[(String, Int, Int), Top2Accum] {

    override def createAccumulator(): Top2Accum = {
      Top2Accum((null, Int.MinValue), (null, Int.MinValue))
    }

    def accumulate(acc: Top2Accum, name: String, price: Int): Unit = {
      if (price > acc.first._2) {
        acc.second = (acc.first._1, acc.first._2)
        acc.first = (name, price)
      } else if (price > acc.second._2) {
        acc.second = (name, price)
      }

    }

    def merge(acc: Top2Accum, it: Iterator[Top2Accum]): Unit = {
      val iterator: Iterator[Top2Accum] = it.toIterator
      while (iterator.hasNext) {
        val top2: Top2Accum = iterator.next()
        accumulate(acc, top2.first._1, top2.first._2)
        accumulate(acc, top2.second._1, top2.second._2)
      }
    }


    def emitValue(acc: Top2Accum, out: Collector[(String, Int, Int)]): Unit = {
      if (acc.first._2 != Int.MinValue) {
        out.collect(acc.first._1, acc.first._2, 1)
      }
      if (acc.second._2 != Int.MinValue) {
        out.collect(acc.second._1, acc.second._2, 2)
      }
    }

  }


}