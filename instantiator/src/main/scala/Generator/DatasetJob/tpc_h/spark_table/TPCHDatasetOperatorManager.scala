package Generator.DatasetJob.tpc_h.spark_table

import Generator.DatasetJob.{AbstractDatasetOperatorManager, AbstractTableOperatorManager, AbstractTableOperatorManagerSpark}

import scala.collection.mutable
import scala.util.Random

/**
  * Created by researchuser7 on 2020-04-20.
  */


object TPCHDatasetOperatorManager extends AbstractDatasetOperatorManager {

  val tables: Seq[String] = Seq(
    "lineitem",
    "orders",
    "customer",
    "partsupp",
    "part",
    "supplier",
    "nation"
  )

  override def getTableOperatorManager(s: String): AbstractTableOperatorManager = s match {
    case "lineitem" => new LineitemOperatorManager() with AbstractTableOperatorManagerSpark
    case "orders" => new  OrdersOperatorManager() with AbstractTableOperatorManagerSpark
    case "customer" => new CustomerOperatorManager() with AbstractTableOperatorManagerSpark
    case "partsupp" => new PartsuppOperatorManager() with AbstractTableOperatorManagerSpark
    case "part" => new PartOperatorManager() with AbstractTableOperatorManagerSpark
    case "supplier" => new SupplierOperatorManager() with AbstractTableOperatorManagerSpark
    case "nation" => new NationOperatorManager() with AbstractTableOperatorManagerSpark
    case _ => throw new Exception(s"Can not find table operator manager for table '$s' ")
  }

  def main(args: Array[String]): Unit = {
    for (i <- 0 to 100){
      try{
        val (tableSequence, joinSequence) = getSourceTableSequence(5, i)
        joinSequence.foreach( j => println(s"> $j"))
        println()
        println(s"Table sequence: $tableSequence")
      } catch {
        case ex:Exception => println(ex)
      } finally {
        println("---------------------------------------------")
      }
    }
  }
}
