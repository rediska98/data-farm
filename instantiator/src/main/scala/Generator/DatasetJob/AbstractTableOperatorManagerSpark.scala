package Generator.DatasetJob

import Generator.DatasetJob.utils.getElementBySeed


trait AbstractTableOperatorManagerSpark extends AbstractTableOperatorManager{

  override def buildDataSourceCode(schema: String, tableName: String, outVarName: String, delimiter: String = "|"): String = {
    s"""
       |val $outVarName = sc.textFile(params("dataPath")+"$tableName").map(f=> f.split('|')).map$schema
       """.stripMargin
  }

  override def buildFilterCode(inVarName: String, outVarName: String, filterField: String, filterValue: String, filterOp: String = "<="): String = {
    s"""
       |val $outVarName = $inVarName.filter(x=>x._$filterField $filterOp $filterValue)
      """.stripMargin
  }

  override def buildReduceCode2(inVarName: String, outVarName1: String, outVarName2: String, groupByField: String): String = {
    s"""
       |val $outVarName1 = $inVarName.keyBy(x => x._$groupByField)
       |val $outVarName2 = $outVarName1.reduceByKey((x1, x2) => x1).map(x => x._2)
      """.stripMargin
  }

  override def buildGroupByCode1(inVarName: String, outVarName: String, groupByField: String): String = {
    s"""
       |val $outVarName = $inVarName.keyBy(x => x._$groupByField).reduceByKey((x1, x2) => x1).map(x => x._2)
          """.stripMargin
  }

  override def buildGroupByCode2(inVarName: String, outVarName1: String, outVarName2: String, groupByField: String): String = {
    //TODO check aggregation function complexity
    s"""
       |val $outVarName1 = $inVarName.keyBy(x => x._$groupByField)
       |val $outVarName2 = $outVarName1.reduceByKey((lx, rx) => rx).map(x => x._2)
          """.stripMargin
  }

  override def buildSortPartitionCode(inVarName: String, outVarName: String, sortField: String, order: String = "true"): String = {
    s"""
       |val $outVarName = $inVarName.sortBy(_._$sortField, $order)
    """.stripMargin
  }

  override def buildJoinCode(lxVarName: String, rxVarName: String, outVarName: String, joinRelation: JoinRelation): String = {
    val lxJFieldId = joinRelation.lx.fields(joinRelation.field).toInt
    val rxJFieldId = joinRelation.rx.fields(joinRelation.field).toInt

    s"""
       |
       |val $outVarName = $lxVarName.keyBy(x => x._$lxJFieldId).join($rxVarName.keyBy(x => x._$rxJFieldId)).map(x => x._2._2)
       """.stripMargin
  }

  override def reduceCode1(inVarName: String, outVarName: String): String = {
    s"""
       |val $outVarName = sc.parallelize(Seq($inVarName.reduce((x1, x2) => x1)))
      """.stripMargin
  }

  override def mapCode(inVarName: String, outVarName: String, seed: Int = 0): (String, Int) = {
    val complexity = seed % 3
    val c = complexity match {
      case 0 => s"""
                   |val $outVarName = $inVarName.map(x=>x)
                """.stripMargin
      case 1 => s"""
                   |val $outVarName = $inVarName.map(x=> {var count = 0; for (v <- x.productIterator) count+=1; x})
                """.stripMargin
      case 2 => s"""
                   |val $outVarName = $inVarName.map(x=> {var count = 0; for (v1 <- x.productIterator; v2 <- x.productIterator) count+=1; x})
       """.stripMargin
    }
    (c, complexity)
  }

  //very useful stackoverflow page about partitioning
  //https://stackoverflow.com/questions/31424396/how-does-hashpartitioner-work
  override def partitionCode(inVarName: String, outVarName: String): String = {
    s"""
       |val $outVarName = $inVarName.keyBy(x => x._1)
       |.partitionBy(new HashPartitioner($inVarName.partitions.length)).map(_._2)
      """.stripMargin

  }

  override def groupByCode1(inVarName: String, outVarName: String, seed: Int): (String, Double) = {
    val gField = getElementBySeed(groupFields.keys.toSeq, seed).toString
    val gFieldId = fields(gField).toInt
    val code = buildGroupByCode1(inVarName, outVarName, gFieldId.toString)
    (code, groupFields(gField).toDouble)
  }

  override def groupByCode2(inVarName: String, outVarName1: String, outVarName2: String, seed: Int): (String, Double) = {
    val gField = getElementBySeed(groupFields.keys.toSeq, seed).toString
    val gFieldId = fields(gField).toInt
    val code = buildGroupByCode2(inVarName, outVarName1, outVarName2, gFieldId.toString)
    (code, groupFields(gField).toDouble)
  }

  override def reduceCode2(inVarName: String, outVarName1: String, outVarName2: String, seed: Int): String = {
    val rField = getElementBySeed(groupFields.keys.toSeq, seed).toString
    val rFieldId = fields(rField).toInt
    val code = buildReduceCode2(inVarName, outVarName1, outVarName2, rFieldId.toString)
    code
  }

  override def sortPartitionCode(inVarName: String, outVarName: String, seed: Int): String = {
    val sField = getElementBySeed(fields.keys.toSeq, seed).toString
    val sFieldId = sortFields(sField)
    buildSortPartitionCode(inVarName, outVarName, sFieldId)
  }


  override def sinkCode(inVarName: String, outVarName: String): String = {
    s"""
       |val $outVarName = $inVarName.count()
       |val cardinality = $outVarName
    """.stripMargin
  }


}
