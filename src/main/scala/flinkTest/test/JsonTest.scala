package flinkTest.test

import java.util

import bean.{Sensor, SensorJava}
import com.alibaba.fastjson.{JSON, JSONObject}
import com.alibaba.fastjson.serializer.SerializeFilter

/**
 * @Auther: wxf
 * @Date: 2022/9/1 19:56:20
 * @Description: Test
 * @Version 1.0.0
 */
object JsonTest {
  def main(args: Array[String]): Unit = {
    //    // 随机数
    //    val random: Random = new Random()
    //    val i: Int = random.nextInt(10) + 1
    //    println(i)
    //
    //    // 随机数 double
    //    val temp1: String = (random.nextDouble() * 100 + 1).formatted("%06.3f") // 整个输出保留6位数长度，且保留3位小数
    //    val temp2: String = (random.nextDouble() * 100 + 1).formatted("%.2f") // 保留2位小数
    //    println(temp1)

    // 对象 转换 Json   使用 fastjson工具实现Json转换
    // 必须要 添加 @BeanProperty 用于 JSON 转换 对象  或者 使用Java类
    val str: String = """{"id":"sensor_1","temperature":35.8,"timestamp":1547718199000}"""

    //    val reading1: Sensor = JSON.parseObject(str, classOf[Sensor])
    //    val reading1: SensorJava = JSON.parseObject(str, classOf[SensorJava])

    val reading1: JSONObject = JSON.parseObject(str)
    //    val id: String = reading1.getString("id")
    //    val timeStamp: Long = reading1.getLong("timeStamp")
    //    val temperature: Double = reading1.getDouble("temperature")
    //    val sensor: Sensor = Sensor(id, timeStamp, temperature)

    val strs: String = JSON.toJSONString(reading1, null.asInstanceOf[Array[SerializeFilter]])
    val str1: String = JSON.toJSONString(reading1, true)
    println(reading1)
    println(strs)
    println(str1)

    //    // scala 自带 Json4s工具
    //    val reading: Sensor = new Sensor("sensor_1", 1547718199000L, 35.8)
    //    // 隐式转换
    //    implicit val f = org.json4s.DefaultFormats
    //    // 对象转换字符串
    //    val str: String = Serialization.write(reading)
    //    println(str)
    //
    //    // 字符串 转换 对象
    //    val reading1: Sensor = JsonMethods.parse(str).extract[Sensor]
    //    println(reading1)

  }
}
