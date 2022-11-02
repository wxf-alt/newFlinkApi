package flinkTest.tableApi.custom_function

import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.{DataTypes, Table, row}
import org.apache.flink.table.functions.AggregateFunction


/**
 * @Auther: wxf
 * @Date: 2022/11/2 10:30:20
 * @Description: WeightedAvgAggregateFunction
 * @Version 1.0.0
 */
object WeightedAvgAggregateFunction {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val table: Table = tableEnv.fromValues(DataTypes.ROW(
      DataTypes.FIELD("id", DataTypes.INT()),
      DataTypes.FIELD("name", DataTypes.STRING()),
      DataTypes.FIELD("price", DataTypes.DOUBLE())),
      row(1, "ABC", 10.2),
      row(2, "ABCDE", 20.1),
      row(3, "ABCDE", 20.3),
      row(4, "ABC", 10.5),
      row(5, "ABE", 20.7),
      row(6, "ABC", 26.4),
      row(7, "ABDE", 30.4),
      row(8, "ABC", 21.2)
    )

    table.select($"*").execute().print()

    tableEnv.registerFunction("wAvg", new WeightedAvg())
    // 使用函数
    tableEnv.sqlQuery(s"SELECT name, wAvg(price) AS avgPoints FROM ${table} GROUP BY name").execute().print()

    table.groupBy($"name")
      .aggregate(call("wAvg", $"price").as("avgPoints"))
      .select($"name", $"avgPoints")
      .execute()
      .print()

    val weightedAvg: WeightedAvg = new WeightedAvg()
    table.groupBy($"name")
      .aggregate(weightedAvg($"price").as("avgPoints"))
      .select($"name", $"avgPoints")
      .execute()
      .print()

  }

  // 累加器类型
  //  case class WeightedAvgAccum(var sum: JDouble, var count: JInteger)
  case class WeightedAvgAccum(var sum: Double, var count: Int)

  class WeightedAvg extends AggregateFunction[Double, WeightedAvgAccum] {

    override def createAccumulator(): WeightedAvgAccum = WeightedAvgAccum(0D, 0)

    override def getValue(accumulator: WeightedAvgAccum): Double = {
      val result: Double = accumulator.sum / accumulator.count
      val double: Double = result.formatted("%.2f").toDouble
      double
    }

    def accumulate(acc: WeightedAvgAccum, num: Double): Unit = {
      acc.sum += num
      acc.count += 1
    }

    def retract(acc: WeightedAvgAccum, num: Double): Unit = {
      acc.sum += num
      acc.count += 1
    }

    def merge(acc: WeightedAvgAccum, it: Iterable[WeightedAvgAccum]): Unit = {
      val iter: Iterator[WeightedAvgAccum] = it.iterator
      while (iter.hasNext) {
        val a: WeightedAvgAccum = iter.next()
        acc.count += a.count
        acc.sum += a.sum
      }
    }

    def resetAccumulator(acc: WeightedAvgAccum): Unit = {
      acc.sum = 0D
      acc.count = 0
    }

    // 设置 返回值 类型
    override def getResultType: TypeInformation[Double] = TypeInformation.of(classOf[Double])

    // 设置 累加器 类型
    override def getAccumulatorType: TypeInformation[WeightedAvgAccum] = TypeInformation.of(classOf[WeightedAvgAccum])

    //    override def getAccumulatorType: TypeInformation[WeightedAvgAccum] = TypeInformation.of(new TypeHint[WeightedAvgAccum] {})
  }

}