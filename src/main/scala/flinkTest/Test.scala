package flinkTest

/**
 * @Auther: wxf
 * @Date: 2022/9/1 19:56:20
 * @Description: Test
 * @Version 1.0.0
 */
object Test {
  def main(args: Array[String]): Unit = {
    val eventTime: Long = 1547718203000L
    val durationMsec: Long = 3600000L
    val endOfWindow: Long = eventTime - (eventTime % durationMsec) + durationMsec - 1

    val l: Long = eventTime % durationMsec
    println(l)
    println(eventTime - l)


    println(endOfWindow)
  }
}
