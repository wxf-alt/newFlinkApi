package bean


/**
 * @Auther: wxf
 * @Date: 2022/9/1 14:44:32
 * @Description: Sensor
 * @Version 1.0.0
 */
case class Sensor(id: String, timeStamp: Long, temperature: Double)

//// 定义样例类，传感器id，时间戳，温度
//// 添加 @BeanProperty 用于 JSON 转换 对象
//@BeanProperty
//case class Sensor(@BeanProperty id: String, @BeanProperty timeStamp: Long, @BeanProperty temperature: Double) {
//  override def toString: String = s"id：${id},timestamp：${timeStamp}，temperature：${temperature}"
//}
