package com.pharbers.StreamEngine.Jobs.DataCleanJob.EditDistanceJob

import com.pharbers.StreamEngine.Utils.Job.{BPSJobContainer, BPStreamJob}
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import com.pharbers.StreamEngine.Jobs.DataCleanJob.EditDistanceJob.BPEditDistance.{TABLE_NAME_CONFIG_DOC, TABLE_NAME_CONFIG_KEY}
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Strategy.BPSDataMartBaseStrategy
import com.pharbers.StreamEngine.Utils.Strategy.JobStrategy.BPSCommonJobStrategy
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.storage.StorageLevel

/** 功能描述
  *
  * @author dcs
  * @version 0.0
  * @since 2020/02/04 13:38
  * @note 一些值得注意的地方
  */
class BPEditDistanceV2(jobContainer: BPSJobContainer, override val componentProperty: Component2.BPComponentConfig)
        extends BPStreamJob {

    override def createConfigDef(): ConfigDef = new ConfigDef()
            .define(TABLE_NAME_CONFIG_KEY, Type.STRING, "cpa", Importance.HIGH, TABLE_NAME_CONFIG_DOC)

    override type T = BPSCommonJobStrategy
    override val strategy: BPSCommonJobStrategy = new BPSCommonJobStrategy(componentProperty, configDef)
    override val jobId: String = strategy.getJobId
    val runId: String = strategy.getRunId
    override val id: String = strategy.getId
    override val spark: SparkSession = strategy.getSpark

    import spark.implicits._

    //todo: 配置传入
    val mappingConfig: Map[String, List[String]] = Map(
        "PRODUCT_NAME" -> List("PROD_NAME_CH"),
        "SPEC" -> List("SPEC"),
        "DOSAGE" -> List("DOSAGE"),
        "PACK_QTY" -> List("PACK"),
        "MANUFACTURER_NAME" -> List("MNF_NAME_CH", "MNF_NAME_EN")
    )

    val joinKey: (String, String) = "MOLE_NAME" -> "MOLE_NAME_CH"

    val minKeys = List("MOLE_NAME", "PRODUCT_NAME", "SPEC", "DOSAGE", "PACK_QTY", "MANUFACTURER_NAME")

    val canReplaceList = List()

    override def open(): Unit = {
        inputStream = jobContainer.inputStream
    }


    override def exec(): Unit = {
        //todo: 配置传入
        val mappingDf = spark.sql("select * from prod")
        inputStream match {
            case Some(in) => check(in, mappingDf)
            case _ =>
        }
    }

    override def close(): Unit = {
        super.close()
        jobContainer.finishJobWithId(id)
    }

    def check(in: DataFrame, checkDf: DataFrame): Unit = {
        val mapping = Map() ++ mappingConfig
//        val tmpPath = "s3a://ph-stream/tmp/editDistance/"
        val moleHumanReplace = spark.sql("select MOLE_NAME as MOLE,  PROD_MOLE_NAME from mole_human_replace where PROD_MOLE_NAME is not null and PROD_MOLE_NAME != '#N/A' and PROD_MOLE_NAME != ''")
        val cpaDf = in.join(moleHumanReplace, $"MOLE_NAME" === $"MOLE", "left")
                .withColumn("MOLE_NAME", when($"MOLE".isNull or $"MOLE" === "", $"MOLE_NAME").otherwise($"PROD_MOLE_NAME"))
                .drop(moleHumanReplace.columns: _*)
        val distanceDf = joinWithCheckDf(cpaDf, checkDf)
        val filterMinDistanceDf = filterMinDistanceRow(mapping, distanceDf)
//        filterMinDistanceDf.write.mode("overwrite").parquet(tmpPath + "filterMinDistanceDf")
        val withHumanReplaceDf = humanReplace(filterMinDistanceDf, mapping)
//        withHumanReplaceDf.write.mode("overwrite").parquet(tmpPath + "withHumanReplaceDf")
        saveTable(withHumanReplaceDf, cpaDf, checkDf, mapping)
        jobContainer.finishJobWithId(id)
    }

    def joinWithCheckDf(in: DataFrame, checkDf: DataFrame): DataFrame = {
        val inProdDf = in.selectExpr(minKeys: _*).distinct()
        val inDfRename = inProdDf.columns.foldLeft(inProdDf)((l, r) =>
            l.withColumnRenamed(r, s"in_$r"))
        val checkDfRename = checkDf.columns.foldLeft(checkDf)((l, r) =>
            l.withColumnRenamed(r, s"check_$r"))
        val moleHumanReplace = spark.sql("select MOLE_NAME,  PROD_MOLE_NAME from mole_human_replace where PROD_MOLE_NAME is not null and PROD_MOLE_NAME != '#N/A' and PROD_MOLE_NAME != ''")

        val joinDf = inDfRename
                .join(moleHumanReplace, $"in_MOLE_NAME" === $"MOLE_NAME", "left")
                .withColumn("in_MOLE_NAME", when($"MOLE_NAME".isNull or $"MOLE_NAME" === "", $"in_MOLE_NAME").otherwise($"PROD_MOLE_NAME"))
                .drop(moleHumanReplace.columns: _*)
                .withColumn("id", monotonically_increasing_id())
                .join(checkDfRename, col("in_MOLE_NAME") === col("check_MOLE_NAME_CH"), "left")
                .na.fill("")


        mappingConfig.foldLeft(joinDf)((l, r) => getColumnDistance(l, r._1, r._2))
                .persist(StorageLevel.MEMORY_AND_DISK_SER)
    }

    def filterMinDistanceRow(mapping: Map[String, List[String]], distance: DataFrame): DataFrame = {
        val distanceWithArray = mapping.keySet.foldLeft(distance)((df, column) =>
            df.withColumn(s"${column}_replaces",
                when(col(s"${column}_distance")(2) > 0, array(col(s"${column}_distance")(1))).otherwise(array())
            ))

        //todo: 这儿filter掉了没有和prod匹配上的，如果逻辑需要保留全部这儿需要去除并且将这儿filter放在后面createReplaceLog的逻辑
        //            filterMin(distanceDf, mappingConfig)
        val filterMinDistanceDf = distanceWithArray
//                .filter("check_MOLE_NAME_CH != ''")
                .groupByKey(x => x.getAs[Long]("id"))
                .mapGroups((_, row) => row.reduce((l, r) => {
                    // val editSumFunc: Row => Int = row => mapping.keySet.toList.foldLeft(0)((x, y) => x + row.getAs[Seq[String]](s"${y}_distance")(2).toInt)
                    val margeRow: (Row, Row, List[String]) => Row = (l, r, checkColumns) => {
                        val editSumFunc: Row => Int = row => mapping.keySet.toList.foldLeft(0)((x, y) => x + row.getAs[Seq[String]](s"${y}_distance")(2).toInt)
                        val map = Map[String, (Row, String) => Any](
                            "string" -> ((row, name) => row.getAs[String](name)),
                            "array" -> ((row, name) => row.getAs[Seq[String]](name)),
                            "integer" -> ((row, name) => row.getAs[Int](name)),
                            "long" -> ((row, name) => row.getAs[Long](name))
                        )
                        val rowLeft = l.schema.map(x => (x.name, map(x.dataType.typeName)(l, x.name)))
                        val rowRight = r.schema.map(x => (x.name, map(x.dataType.typeName)(r, x.name)))
                        val isMore = editSumFunc(l) > editSumFunc(r)
                        val row = rowLeft.zip(rowRight).map { case (colL, colR) =>
                            if (checkColumns.contains(colL._1)) {
                                (colL._2.asInstanceOf[Seq[String]] ++ colR._2.asInstanceOf[Seq[String]]).distinct
                            } else {
                                if (isMore) colR._2 else colL._2
                            }
                        }
                        new GenericRowWithSchema(row.toArray, l.schema)
                    }
                    val sameColSumFunc: Row => Int = row => mapping.keySet.toList.map(x => row.getAs[Seq[String]](s"${x}_distance")(2).toInt).count(x => x <= 0)
                    sameColSumFunc(l).compareTo(sameColSumFunc(r)) match {
                        case 0 => margeRow(l, r, mapping.keySet.toList.map(x => s"${x}_replaces"))
                        case 1 => l
                        case -1 => r
                    }
                }))(RowEncoder(distanceWithArray.schema))
                .persist(StorageLevel.MEMORY_AND_DISK_SER)
        distance.unpersist(true)
        filterMinDistanceDf.write.mode("overwrite").option("path", "s3a://ph-stream/test/filterMinDistanceDf").saveAsTable("filterMinDistanceDf")
        filterMinDistanceDf
    }

    def humanReplace(filterMinDistanceDf: DataFrame, mapping: Map[String, List[String]]): DataFrame = {
        val keys = minKeys
        val humanDf = spark.sql("select * from human_replace")
                .join(spark.sql("select PACK_ID, MOLE_NAME_CH, PROD_NAME_CH, SPEC as SPEC_PROD, DOSAGE as DOSAGE_PROD, PACK, MNF_NAME_CH from prod"),
            $"MOLE_NAME" === $"MOLE_NAME_CH"
                    and $"PRODUCT_NAME" === $"PROD_NAME_CH"
                    and $"SPEC" === $"SPEC_PROD"
                    and $"DOSAGE" === $"DOSAGE_PROD"
                    and $"PACK_QTY" === $"PACK"
                    and $"MANUFACTURER_NAME" === $"MNF_NAME_CH", "left")
                .drop("MOLE_NAME_CH", "SPEC_PROD", "DOSAGE_PROD", "PACK", "MNF_NAME_CH")

        val joinDf = filterMinDistanceDf
                .withColumn("in_min", concat(keys.map(x => col(s"in_$x")): _*))
                .join(humanDf, $"in_min" === $"min", "left")
        val withHumanReplaceDf = mapping.keys.foldLeft(joinDf)((df, key) => {
            df.withColumn(s"${key}_distance", when($"min".isNull, col(s"${key}_distance"))
                    .otherwise(array(
                        col(s"${key}_distance")(0),
                        when(col(s"$key") === "", col(s"${key}_distance")(1)).otherwise(col(s"$key")),
                        when(col(s"${key}_distance")(0) === col(s"$key"), lit("0"))
                                .otherwise(when(col(s"$key") === "", col(s"${key}_distance")(2)).otherwise(lit("0"))))
                    )
            )
        })
                .withColumn("check_PACK_ID", when($"PACK_ID".isNull, $"check_PACK_ID").otherwise($"PACK_ID"))
                .drop(humanDf.columns: _*)
                .persist(StorageLevel.MEMORY_AND_DISK_SER)
        filterMinDistanceDf.unpersist(true)
        withHumanReplaceDf.write.mode("overwrite")
                .option("path", "s3a://ph-stream/test/withHumanReplaceDf").saveAsTable("withHumanReplaceDf")
        withHumanReplaceDf
    }

    private def getColumnDistance(df: DataFrame, column: String, checkColumns: List[String]): DataFrame = {
        val checkUdf = udf((x: String, y: String, col: String) => BPEditDistanceV2.getCheckRes(x, y, col))
        val min = udf((array: Seq[Seq[String]]) => BPEditDistanceV2.minDistanceArray(array))
        df.na.fill("")
                .withColumn(s"${column}_distance", min(array(checkColumns.map(x =>
                    checkUdf(col(s"in_$column"), col(s"check_$x"), lit(column))): _*)))
    }

    private def replaceWithDistance(columnName: String, df: DataFrame): DataFrame = {
        val replaceUdf = udf((distance: Seq[String], replaceSize: Int) => BPEditDistanceV2.replaceFunc(distance, replaceSize))
        val replaceSizeCol = if(canReplaceList.contains(columnName)) col(s"replace_size") else lit(100)
        df.withColumn(s"$columnName", replaceUdf(col(s"${columnName}_distance"), replaceSizeCol))
    }

    private def createReplaceLog(replaceDf: DataFrame, inDfColumns: Array[String], mapping: Map[String, List[String]]): DataFrame = {
        val canReplaceListCp = List(canReplaceList: _*)
        replaceDf
                .selectExpr(Seq("id")
                        ++ mapping.keys.map(x => s"${x}_distance").toSeq
                        ++ mapping.keys.map(x => s"${x}_replaces").toSeq
                        ++ inDfColumns.map(x => s"$x").toSeq: _*
                )
                .withColumn("distances", map(mapping.keys.toSeq.flatMap(key => List(lit(key), col(s"${key}_distance"))): _*))
                .withColumn("replaces", map(mapping.keys.toSeq.flatMap(key => List(lit(key), col(s"${key}_replaces"))): _*))
                .withColumn("cols", array(inDfColumns.map(x => col(s"$x")).toSeq: _*))
                .select("id", "distances", "cols", "replaces")
                .flatMap {
                    case Row(id: Long, distances: Map[String, Seq[String]], cols: Seq[String], replaces: Map[String, Seq[String]]) =>
                        distances.map{case (colName, colDistances) =>
                            val colValue = colDistances.head
                            val colDistance = colDistances(2).toInt
                            val checkValue = colDistances(1)
                            val canReplace = (canReplaceListCp.contains(colName) && replaces.map(x => x._2.length).sum <= 1) || math.ceil(colValue.length / 5.0).toInt >= colDistance
                            (id, colName, canReplace, colValue, checkValue, colDistance, cols, replaces(colName))
                        }
                }
                .toDF("ID", "COL_NAME", "canReplace", "ORIGIN", "check", "distance", "cols", "CANDIDATE")
    }

    private def saveTable(withHumanReplaceDf: DataFrame, inDf: DataFrame, checkDf: DataFrame, mapping: Map[String, List[String]]): Unit = {
        val prodKeys = minKeys
        val inDfWithDistance = inDf
                .na.fill("")
                .withColumn("min", concat(prodKeys.map(x => col(s"$x")): _*))
                .join(withHumanReplaceDf, $"min" === $"in_min")
                .withColumn("id", monotonically_increasing_id()) //之前创建的id是一个产品一个，现在是一条记录一个
                .persist(StorageLevel.MEMORY_AND_DISK_SER)
        withHumanReplaceDf.unpersist(true)
        inDfWithDistance.write.mode("overwrite").option("path", "s3a://ph-stream/test/inDfWithDistance").saveAsTable("inDfWithDistance")
        val replaceLogDf = createReplaceLog(inDfWithDistance, inDf.columns, mapping)
        val tableName = strategy.getJobConfig.getString(BPEditDistanceV2.TABLE_NAME_CONFIG_KEY)
        val mode = "overwrite"
        val saveHandler = new TableSaveHandler(inDf, checkDf, tableName)
        saveHandler.saveReplaceTable(replaceLogDf, mode)
        saveHandler.saveNoReplaceTable(replaceLogDf, mode)
        val withReplaceSizeDf = inDfWithDistance.withColumn("replace_size", expr(mapping.keys.map(x => s"size(${x}_replaces)").mkString(" + ")))
        saveHandler.saveNewTable(mapping.keys.foldLeft(withReplaceSizeDf)((df, s) => replaceWithDistance(s, df)), mode)
        inDfWithDistance.unpersist(true)
    }

    override val description: String = "EditDistanceJob"

    class TableSaveHandler(inDf: DataFrame, checkDf: DataFrame, tableName: String) {
        lazy val inVersion: String = inDf.select("version").take(1).head.getAs[String]("version")
        lazy val checkVersion: String = checkDf.select("version").take(1).head.getAs[String]("version")
        lazy val dataMartStrategy: BPSDataMartBaseStrategy = BPSDataMartBaseStrategy(componentProperty)

        def saveNewTable(df: DataFrame, mode: String): Unit = {
            val newTableName = s"${tableName}_new"
            val version = getVersion(newTableName, mode)
            val url = getTableSavePath(newTableName, inVersion, checkVersion, version)
            df.selectExpr(Array("id", "check_PACK_ID as PACK_ID") ++ inDf.columns: _*)
                    .withColumn("version", lit(version))
                    .write
                    .mode(mode)
                    .option("path", url)
                    .saveAsTable(newTableName)
            dataMartStrategy.pushDataSet(newTableName, version, url, mode, jobId, strategy.getTraceId, Nil)
        }

        def saveReplaceTable(df: DataFrame, mode: String): Unit = {
            val replaceTableName = s"${tableName}_replace"
            val version = getVersion(replaceTableName, mode)
            val url = getTableSavePath(replaceTableName, inVersion, checkVersion, version)
            df.filter("canReplace = true and ORIGIN != check")
                    .selectExpr(List("ID", "COL_NAME", "ORIGIN", "check as DEST") ++ inDf.columns.zipWithIndex.map(x => s"cols[${x._2}] as ORIGIN_${x._1}").toList: _*)
                    .withColumn("version", lit(version))
                    .write
                    .mode(mode)
                    .option("path", url)
                    .saveAsTable(replaceTableName)
            dataMartStrategy.pushDataSet(replaceTableName, version, url, mode, jobId, strategy.getTraceId, Nil)
        }

        def saveNoReplaceTable(df: DataFrame, mode: String): Unit = {
            val noReplaceLogTableName = s"${tableName}_no_replace"
            val version = getVersion(noReplaceLogTableName, mode)
            val url = getTableSavePath(noReplaceLogTableName, inVersion, checkVersion, version)
            df.filter("canReplace = false")
                    .selectExpr(List("ID", "COL_NAME", "ORIGIN", "CANDIDATE", "DISTANCE")
                            ++ inDf.columns.zipWithIndex.map(x => s"cols[${x._2}] as ORIGIN_${x._1}").toList: _*)
                    .withColumn("version", lit(version))
                    .write
                    .mode(mode)
                    .option("path", url)
                    .saveAsTable(noReplaceLogTableName)
            dataMartStrategy.pushDataSet(noReplaceLogTableName, version, url, mode, jobId, strategy.getTraceId, Nil)
        }
    }

}

object BPEditDistanceV2 extends Serializable {
    final val TABLE_NAME_CONFIG_KEY = "tableName"
    final val TABLE_NAME_CONFIG_DOC = "need check table name"
    //    final val DATA_SETS_CONFIG_KEY = "dataSets"
    //    final val DATA_SETS_CONFIG_DOC = "dataSet ids"

    def checkColumnsFunc(map: Map[String, Int]): Boolean = {
        map.forall(r => (r._1.length / 5 + 1) >= r._2)
    }

    def replaceFunc(distance: Seq[String], replaceSize: Int): String = {
        if (((distance.head.length / 5 + 1) >= distance(2).toInt || replaceSize <= 1) && distance(1) != "") {
            distance(1)
        } else distance.head
    }

    def minDistanceArray(array: Seq[Seq[String]]): Seq[String] = {
        array.minBy(x => x(2).toInt)
    }

    def getCheckRes(inputWord: String, targetWord: String, colName: String): Array[String] ={
        def replaceAndContains(s1: String, s2: String): Boolean ={
            val list = List(
                "股份", "有限", "公司", "集团", "制药", "厂", "药业", "责任", "健康", "科技", "生物", "工业"
            )
            val manufacturer1 = list.foldLeft(s1)((s, r) => s.replaceAll(r, ""))
            val manufacturer2 = list.foldLeft(s2)((s, r) => s.replaceAll(r, ""))
            manufacturer1.contains(manufacturer2) || manufacturer2.contains(manufacturer1)
        }
        def checkSep(s1: String, s2: String): Boolean = {
            case class SpecNum(unit: String, value: String, `type`: String){
                override def equals(obj: Any): Boolean = {
                    if(!obj.isInstanceOf[SpecNum]) return super.equals(obj)
                    val specNumObj = obj.asInstanceOf[SpecNum]
                    if(`type` == specNumObj.`type`){
                        if(unit != specNumObj.unit || `type` != "other"){
                            `type` match {
                                case "g" =>
                                    val transform: (String, String) => Double = (s, unit) => unit match {
                                        case "G" => s.toDouble * 1000 * 1000
                                        case "GM" => s.toDouble * 1000 * 1000
                                        case "MG" => s.toDouble * 1000
                                        case "UG" => s.toDouble
                                        case "ΜG" => s.toDouble
                                        case "_" => s.toDouble
                                    }
                                    transform(value, unit) == transform(specNumObj.value, specNumObj.unit)
                                case "l" => value.toDouble == specNumObj.value.toDouble
                                case "%" => value.toDouble == specNumObj.value.toDouble
                                case _ => value == specNumObj.value
                            }
                        } else {
                            value == specNumObj.value
                        }
                    } else {
                        false
                    }
                }
                def transform(): SpecNum ={
                    val transformValue = `type` match {
                        case "g" =>
                            unit match {
                                case "G" => value.toDouble * 1000 * 1000
                                case "GM" => value.toDouble * 1000 * 1000
                                case "MG" => value.toDouble * 1000
                                case "UG" => value.toDouble
                                case "ΜG" => value.toDouble
                                case "_" => value.toDouble
                            }
                        case _ => value == value
                    }
                    val transformUnit = `type` match {
                        case "g" => "UG"
                        case _ => unit
                    }
                    SpecNum(transformUnit, transformValue.toString, unit)
                }
            }
            def unitTransform(s: String): SpecNum = {
                try {
                    //todo: 会匹配出.156xxx这样的
                    val num = "[0-9.]".r.findAllIn(s).mkString("")
                    val unit = s.substring(s.indexOf(num)).replaceAll("[0-9.]", "")
                    val `type` = unit match {
                        case "G" => "g"
                        case "GM" => "g"
                        case "MG" => "g"
                        case "UG" => "g"
                        case "ΜG" => "g"
                        case "ML" => "l"
                        case "%" => "%"
                        case _ => "other"
                    }
                    SpecNum(unit, num.toDouble.toString, `type`)
                } catch {
                    case _: Exception => SpecNum("", s, "other")
                }
            }
            def transForm2Percent(array: Array[SpecNum]): Array[SpecNum] ={
                val input = array.filter(x => x.`type` != "other")
                if(input.length < 2) return array
                val sort = if(input.groupBy(x => x.`type`).size == 1){
                    input.map(x => x.transform()).sortBy(x => x.value.toDouble)
                } else {
                    input.map(x => {
                        if(x.`type` == "g"){
                            val transform = x.transform()
                            SpecNum("G", (transform.value.toDouble / 1000 / 1000).toString, x.`type`)
                        } else {
                            x
                        }
                    }).sortBy(x => x.value.toDouble)
                }
                val percent = (sort.head.value.toDouble / sort.last.value.toDouble * 100).toString
                array :+ SpecNum("%", percent, "%")
            }
            val regex = """[0-9][0-9.]*[A-Za-z%\u4e00-\u9fa5]*""".r
            val list1 = regex.findAllIn(s1.toUpperCase()).toArray.filter(x => x != "").map(unitTransform)
            val list2 = s2.toUpperCase().split("[^A-Za-z0-9_.%\\u4e00-\\u9fa5]", -1).filter(x => x != "").map(unitTransform)
            val length = if(list2.exists(x => x.`type` == "%") && !list1.exists(x => x.`type` == "%")){
                transForm2Percent(list1).count(x => list2.contains(x))
            } else {
                list1.count(x => list2.contains(x))
            }
            length >= list2.length || length >= list1.length
        }
        val default:(String,String) => Boolean = (_, _) =>  false
        val map: Map[String, (String, String) => Boolean] = Map(
            "DOSAGE" -> ((s1: String, s2: String) => s1.contains(s2)),
            "PRODUCT_NAME" -> ((s1: String, s2: String) => s1.contains(s2)),
            "MANUFACTURER_NAME" -> ((s1: String, s2: String) => replaceAndContains(s1, s2)),
            "SPEC" -> ((s1: String, s2: String) => checkSep(s1, s2))
        )

        if(map.getOrElse(colName, default)(inputWord.replaceAll(" ", "").toUpperCase, targetWord.toUpperCase)){
            Array(inputWord, targetWord, "0")
        } else {
            getDistance(inputWord, targetWord, colName)
        }
    }

    def getDistance(inputWord: String, targetWord: String, colName: String): Array[String] = {
        val map = Map(
            "PRODUCT_NAME" -> 1,
            "MANUFACTURER_NAME" -> 1,
            "DOSAGE" -> 1,
            "SPEC" -> 10,
            "PACK_QTY" -> 100
        )
        val s1 = inputWord.replaceAll(" ", "").toUpperCase
        val s2 = targetWord.replaceAll(" ", "").toUpperCase
        val resContainer = Array.fill(s1.length + 1, s2.length + 1)(-1)
        val distanceNum = if (inputWord != targetWord && (s1 == s2 || checkSep(inputWord, targetWord))) 0 else distance(s1, s2, s1.length, s2.length, resContainer)
        Array(inputWord, targetWord, (map.getOrElse(colName, 1) * distanceNum).toString)
    }

    private def checkSep(s1: String, s2: String): Boolean = {
        val list1 = s1.toUpperCase().split("[^A-Za-z0-9_.\\u4e00-\\u9fa5]", -1).sorted
        val list2 = s2.toUpperCase().split("[^A-Za-z0-9_.\\u4e00-\\u9fa5]", -1).sorted
        list1.sameElements(list2)
    }

    private def distance(x: String, y: String, i: Int, j: Int, resContainer: Array[Array[Int]]): Int = {
        if (resContainer(i)(j) != -1) return resContainer(i)(j)
        if (i == 0 || j == 0) {
            return Math.max(i, j)
        }

        val replaceRes = if (x.charAt(i - 1) == y.charAt(j - 1)) distance(x, y, i - 1, j - 1, resContainer) else distance(x, y, i - 1, j - 1, resContainer) + 1
        val removeRes = Math.min(distance(x, y, i, j - 1, resContainer), distance(x, y, i - 1, j, resContainer)) + 1
        val res = Math.min(removeRes, replaceRes)
        resContainer(i)(j) = res
        res
    }

}

