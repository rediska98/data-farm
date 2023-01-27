import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io.PrintWriter

import TPC_H.TPC_H_utils


import org.apache.spark.sql.SparkSession
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import java.sql.Date

object q17 {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("TPC-H q17 rdd")
      .getOrCreate()


    spark.sparkContext.setLogLevel("ERROR")

    val lineitem = spark.sparkContext.textFile("/home/denis/IdeaProjects/data-farm/input_data/tpc-h/lineitem.tbl")
    val part = spark.sparkContext.textFile("/home/denis/IdeaProjects/data-farm/input_data/tpc-h/part.tbl")

    val lineitem_data = lineitem.map(f=> f.split("\\|"))
      .map(f => (f(0).toInt, f(1).toInt, f(2).toInt, f(3).toInt, f(4).toFloat, f(5).toFloat, f(6).toFloat, f(7).toFloat, f(8), f(9), f(10), f(11), f(12), f(13), f(14), f(15)))
    val part_data = part.map(f=> f.split("\\|"))
      .map(f => (f(0).toInt, f(1), f(2), f(3), f(4), f(5).toInt, f(6), f(7).toFloat, f(8)))


    val partFiltered = part_data.filter(x => (x._4 == "Brand#23") && (x._7 == "MED BOX"))


    val partFilteredKeyed = partFiltered.keyBy(x => x._1)
    val lineitem_dataKeyed = lineitem_data.keyBy(x => x._2)

    val smallLookup = spark.sparkContext.broadcast(partFilteredKeyed.collect.toMap)

    val l_p_Joined = lineitem_dataKeyed.flatMap { case(key, value) =>
      smallLookup.value.get(key).map { otherValue =>
        (key, (value, otherValue))
      }
    }

    val innerQueryResLookup = l_p_Joined
      .map(x => (x._2._2._1, (x._2._1._5, 1)))
      .reduceByKey((x1, x2) => (x1._1 + x2._1, x1._2 + x2._2))
      .map(x => (x._1, 0.2 * x._2._1 / x._2._2))


    val broadcastSet = spark.sparkContext.broadcast(innerQueryResLookup.collect.toMap)

    val l_p_JoinedMapped = l_p_Joined.map(x => (x._2._2._1, x._2._1._5, x._2._1._6))
//
    val l_p_Filtered = l_p_JoinedMapped.filter(x => x._2 < broadcastSet.value.getOrElse(x._1, 0.0))// we assume that < 0.0 will always give us "false"
//
    val queryResult = l_p_Filtered.map(_._3).reduce(_ + _) / 7.0



    //print debugString to file
    new PrintWriter("q17rddJobToDebugString.txt") { write(l_p_Filtered.toDebugString); close }

    //printing to stdout
    //val dataColl = result.take(10)
    //dataColl.foreach(println)
    println(queryResult)
    spark.sparkContext.stop()
  }
}