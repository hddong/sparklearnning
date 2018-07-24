import java.util.Date

object TimeTest {
  def main(args: Array[String]): Unit = {
    println(System.currentTimeMillis())
    println(new Date().getTime)
  }
}
