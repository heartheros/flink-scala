package Retail

import java.io.FileInputStream
import java.util.Properties

/**
 * @author Leixinxin
 * @date 2020/11/9 3:42 PM
 */
object TestReadConfig {
  def main(args: Array[String]): Unit = {
    var dbProps = new Properties()
    dbProps.load(
      new FileInputStream(Thread.currentThread()
        .getContextClassLoader
        .getResource("config.properties")
        .getPath)
    )
    val a: String = dbProps.getProperty("test")
    val items: Set[String] = a.split(",").toSet

    print(items)
    if (items.contains("1")) {
      print("has 1" + "!")
    } else {
      print("1 not exist")
    }

    if (items.contains("5")) {
      print("has 5" + "!")
    } else {
      print("5 not exist")
    }
  }
}
