/* Q1_Job.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io.PrintWriter

import TPC_H.TPC_H_utils


import org.apache.spark.sql.SparkSession
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import java.sql.Date

object q1 {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val lineitem = sc.textFile("/home/denis/IdeaProjects/data-farm/input_data/tpc-h/lineitem.tbl")
    val data = lineitem.map(f=> f.split('|')).map(f => (f(0).toInt, f(1).toInt, f(2).toInt, f(3).toInt, f(4).toFloat, f(5).toFloat, f(6).toFloat, f(7).toFloat, f(8), f(9), java.sql.Date.valueOf(f(10)), java.sql.Date.valueOf(f(11)), java.sql.Date.valueOf(f(12)), f(13), f(14)))
    val filteredData = data.filter(x => x._11.getTime <= (TPC_H_utils.formatter.parseDateTime("1998-12-01").getMillis - 90L * 24L * 60L * 60L * 1000L)) // 90*24*60*60*1000 -> 90 days
    val groupedData = filteredData
      .map(x => (
        (x._9, // l_returnflag
        x._10), // l_linestatus
        (x._5, // l_quantity
        x._6, // l_extendedprice
        x._6 * (1 - x._7), // l_extendedprice * (1 - l_discount)
        x._6 * (1 - x._7) * (1 + x._8), // l_extendedprice * (1 - l_discount) * (1 + l_tax)
        x._5, // l_quantity
        x._6, // l_extendedprice
        x._7, // l_discount
        1) // count()
      ))
    val aggregatedResult = groupedData
      .reduceByKey((x1, x2) =>
        (
          x1._1 + x2._1, // sum(l_quantity)
          x1._2 + x2._2, // sum(l_extendedprice)
          x1._3 + x2._3, // sum(l_extendedprice * (1 - l_discount))
          x1._4 + x2._4, //sum(l_extendedprice * (1 - l_discount) * (1 + l_tax))
          x1._5 + x2._5, // sum(l_quantity)
          x1._6 + x2._6, // sum(l_extendedprice)
          x1._7 + x2._7, // sum(l_discount)
          x1._8 + x2._8 // count()
        ))
      .map {
        case ((k1, k2), (v1, v2, v3, v4, v5, v6, v7, v8)) => (k1, k2, v1, v2, v3, v4, v5, v6, v7, v8)
      }.map(x => x.copy(_7 = x._7 / x._10, _8 = x._8 / x._10, _9 = x._9 / x._10))

    val sortedResult = aggregatedResult.sortBy(_._2).sortBy(_._1)



    //print debugString to file
    new PrintWriter("q1rddJobToDebugString.txt") { write(sortedResult.toDebugString); close }

    //printing to stdout
    val dataColl=sortedResult.take(10)


    dataColl.foreach(println)
    sc.stop()

  }
}