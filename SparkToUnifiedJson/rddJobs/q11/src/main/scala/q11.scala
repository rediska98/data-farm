/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io.PrintWriter

import TPC_H.TPC_H_utils


import org.apache.spark.sql.SparkSession
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import java.sql.Date

object q11 {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("TPC-H q11 rdd")
      .getOrCreate()


    spark.sparkContext.setLogLevel("ERROR")

    val partsupp = spark.sparkContext.textFile("/home/denis/IdeaProjects/data-farm/input_data/tpc-h/partsupp.tbl")
    val supplier = spark.sparkContext.textFile("/home/denis/IdeaProjects/data-farm/input_data/tpc-h/supplier.tbl")
    val nation = spark.sparkContext.textFile("/home/denis/IdeaProjects/data-farm/input_data/tpc-h/nation.tbl")

    val partsupp_data = partsupp.map(f=> f.split("\\|"))
      .map(f => (f(0).toInt, f(1).toInt, f(2).toInt, f(3).toFloat, f(4)))
    val supplier_data = supplier.map(f=> f.split("\\|"))
      .map(f => (f(0).toInt, f(1), f(2), f(3).toInt, f(4), f(5).toFloat, f(6)))
    val nation_data = nation.map(f=> f.split("\\|"))
      .map(f => (f(0).toInt, f(1), f(2).toInt, f(3)))

    val nationFiltered = nation_data.filter(x => x._2 == "GERMANY")
    val nationFilteredKeyed = nationFiltered.keyBy(x => x._1)
    val supplier_dataKeyed = supplier_data.keyBy(x => x._4)

    //preps for join
    val s_n_joined = supplier_dataKeyed.join(nationFilteredKeyed).map(x => (x._2._1._1, x._2._1._4))
    val s_n_joinedKeyed = s_n_joined.keyBy(x => x._1)

    val partsupp_dataKeyed = partsupp_data.keyBy(x => x._2)

    val ps_s_n_joined = partsupp_dataKeyed.join(s_n_joinedKeyed).map(x => (x._2._1._1, x._2._1._4 * x._2._1._3))

    val havingQueryResult = ps_s_n_joined
      .map(_._2)
      .reduce(_ + _) * 0.00010f

    val ps_s_n_AggregatedData = ps_s_n_joined.reduceByKey((x1,x2) => (x1 + x2))


    val ps_s_n_FilteredAggregatedData = ps_s_n_AggregatedData.filter(x => x._2 > havingQueryResult )

    val sortedData = ps_s_n_FilteredAggregatedData.sortBy(_._2, false)





    //print debugString to file
    new PrintWriter("q11rddJobToDebugString.txt") { write(sortedData.toDebugString); close }

    //printing to stdout
    val dataColl = sortedData.collect()
    dataColl.foreach(println)

    spark.sparkContext.stop()

  }
}