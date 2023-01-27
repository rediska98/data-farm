/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io.PrintWriter

import TPC_H.TPC_H_utils


import org.apache.spark.sql.SparkSession
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import java.sql.Date

object q3 {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("TPC-H q3 rdd")
      .getOrCreate()


    spark.sparkContext.setLogLevel("ERROR")

    val lineitem = spark.sparkContext.textFile("/home/denis/IdeaProjects/data-farm/input_data/tpc-h/lineitem.tbl")
    val customer = spark.sparkContext.textFile("/home/denis/IdeaProjects/data-farm/input_data/tpc-h/customer.tbl")
    val orders = spark.sparkContext.textFile("/home/denis/IdeaProjects/data-farm/input_data/tpc-h/orders.tbl")

    val lieneitem_data = lineitem.map(f=> f.split("\\|"))
      .map(f => (f(0), f(1), f(2), f(3), f(4), f(5).toFloat, f(6).toFloat, f(7), f(8), f(9), f(10), f(11), f(12), f(13), f(14)))
    val customer_data = customer.map(f=> f.split("\\|"))
      .map(f => (f(0), f(1), f(2), f(3), f(4), f(5), f(6), f(7)))
    val orders_data = orders.map(f=> f.split("\\|"))
      .map(f => (f(0), f(1), f(2), f(3), f(4), f(5), f(6), f(7), f(8)))

    val linitemFiltered = lieneitem_data.filter(x => x._11 > "1995-03-15")
    val customerFiltered = customer_data.filter(x => x._7 == "BUILDING")
    val ordersFiltered = orders_data.filter(x => x._5 < "1995-03-15")

    val ordersFilteredv2 = ordersFiltered.map(x => ((x._2), (x._1, x._5, x._8)))
    //preps for join
    val c_o_joined = customerFiltered.map(x => (x._1,x)).join(ordersFilteredv2).map(x => x._2._2) // TODO do map before join
    val l_c_o_joined = c_o_joined.map(x => (x._1,x)).join(linitemFiltered.map(x => (x._1,x))).map(x => (x._2._2._1, x._2._2._6 * (1 - x._2._2._7) ,x._2._1._2,x._2._1._3))
    val aggredatedData = l_c_o_joined.map(x => ((x._1, x._3, x._4),(x._2))).reduceByKey((x1,x2) => (x1 + x2))

    val sortedData = aggredatedData.sortBy(_._1._2).sortBy(_._2, false)





    //print debugString to file
    new PrintWriter("q3rddJobToDebugString.txt") { write(sortedData.toDebugString); close }

    //printing to stdout
    val dataColl=sortedData.take(10)
    dataColl.foreach(println)

    sortedData.count().saveAsTextFile("newfile")

    spark.sparkContext.stop()
  }
}