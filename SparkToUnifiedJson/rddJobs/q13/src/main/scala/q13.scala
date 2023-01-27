import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io.PrintWriter

import TPC_H.TPC_H_utils


import org.apache.spark.sql.SparkSession
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import java.sql.Date

object q13 {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("TPC-H q13 rdd")
      .getOrCreate()


    spark.sparkContext.setLogLevel("ERROR")

    val customer = spark.sparkContext.textFile("/home/denis/IdeaProjects/data-farm/input_data/tpc-h/customer.tbl")
    val orders = spark.sparkContext.textFile("/home/denis/IdeaProjects/data-farm/input_data/tpc-h/orders.tbl")

    val customer_data = customer.map(f=> f.split("\\|"))
      .map(f => (f(0).toInt, f(1), f(2), f(3).toInt, f(4), f(5).toFloat, f(6), f(7)))
    val orders_data = orders.map(f=> f.split("\\|"))
      .map(f => (f(0).toInt, f(1).toInt, f(2), f(3).toFloat, f(4), f(5), f(6), f(7).toInt, f(8)))


    val ordersFiltered = orders_data.filter(x => !x._9.matches(".*special.*requests.*"))


    val ordersFilteredKeyed = ordersFiltered.keyBy(x => x._2)
    val customer_dataKeyed = customer_data.keyBy(x => x._1)

    val c_o_Joined = customer_dataKeyed.leftOuterJoin(ordersFilteredKeyed).map(x => (x._2._1._1, if ((!x._2._2.isEmpty) && !x._2._2.get._1.isNaN) 1 else 0))

    val matTable = c_o_Joined.reduceByKey((x1,x2) => (x1 + x2))

    val matTableAggregated = matTable.map(x => (x._2, 1)).reduceByKey((x1,x2) => (x1 + x2))
    //preps for join

    val sortedData = matTableAggregated.sortBy(_._1, false).sortBy(_._2, false)





    //print debugString to file
    new PrintWriter("q13rddJobToDebugString.txt") { write(sortedData.toDebugString); close }

    //printing to stdout
    val dataColl = sortedData.collect()
    dataColl.foreach(println)

    spark.sparkContext.stop()

  }
}