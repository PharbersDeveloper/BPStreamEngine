package com.pharbers.StreamEngine.Jobs.EditDistanceJob

import com.pharbers.StreamEngine.Utils.Job.{BPSJobContainer, BPStreamJob}
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import com.pharbers.StreamEngine.Jobs.EditDistanceJob.BPEditDistance.{DATA_SETS_CONFIG_DOC, DATA_SETS_CONFIG_KEY, TABLE_NAME_CONFIG_DOC, TABLE_NAME_CONFIG_KEY}
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Strategy.JobStrategy.BPSCommonJobStrategy
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

/** 功能描述
  *
  * @author dcs
  * @version 0.0
  * @since 2020/02/04 13:38
  * @note 一些值得注意的地方
  */
class BPEditDistance(jobContainer: BPSJobContainer, override val componentProperty: Component2.BPComponentConfig)
        extends BPStreamJob {

    override def createConfigDef(): ConfigDef = new ConfigDef()
            .define(TABLE_NAME_CONFIG_KEY, Type.STRING, "cpa", Importance.HIGH, TABLE_NAME_CONFIG_DOC)
            .define(DATA_SETS_CONFIG_KEY, Type.LIST, Importance.HIGH, DATA_SETS_CONFIG_DOC)

    override type T = BPSCommonJobStrategy
    override val strategy: BPSCommonJobStrategy = new BPSCommonJobStrategy(componentProperty, configDef)
    val jobId: String = strategy.getJobId
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

    override def open(): Unit = {
        //                inputStream = Some(spark.sql("select * from cpa"))
        inputStream = jobContainer.inputStream
    }


    override def exec(): Unit = {
        //todo: 配置传入
        val mappingDf = spark.sql("select * from prod")
        inputStream match {
            case Some(in) => check(in.repartition(2000), mappingDf)
            case _ =>
        }
    }

    override def close(): Unit = {
        super.close()
        jobContainer.finishJobWithId(id)
    }

    def check(in: DataFrame, checkDf: DataFrame): Unit = {
        val mapping = Map() ++ mappingConfig
       joinWithCheckDf(in, checkDf, s"/user/dcs/test/tmp/distanceDf_$id")
       filterMinDistanceRow(mapping, spark.read.parquet(s"/user/dcs/test/tmp/distanceDf_$id"), s"/user/dcs/test/tmp/res_${id}")
        val filterMinDistanceDf = humanReplace(spark.read.parquet(s"/user/dcs/test/tmp/res_${id}"), mapping).cache()
        saveTable(filterMinDistanceDf, in, checkDf, mapping)
    }

    def joinWithCheckDf(in: DataFrame, checkDf: DataFrame, path: String): Unit = {
        val repartitionByIdNum = 50
        val inDfRename = in.columns.foldLeft(in)((l, r) =>
            l.withColumnRenamed(r, s"in_$r"))
        val checkDfRename = checkDf.columns.foldLeft(checkDf)((l, r) =>
            l.withColumnRenamed(r, s"check_$r"))

        val joinDf = inDfRename
                .withColumn("id", monotonically_increasing_id())
                .withColumn("partition_id", $"id" % repartitionByIdNum)
                .join(checkDfRename, col("in_MOLE_NAME") === col("check_MOLE_NAME_CH"), "left")
                .na.fill("")

        val distanceDfTmp = mappingConfig.foldLeft(joinDf)((l, r) => getColumnDistance(l, r._1, r._2))
        //                .persist(StorageLevel.DISK_ONLY_2)

        distanceDfTmp.write
                .partitionBy("partition_id")
                .mode("overwrite")
                .parquet(path)
    }

    def filterMinDistanceRow(mapping: Map[String, List[String]], distance: DataFrame, path: String): Unit = {
        //        val distance = spark.read.parquet(s"/user/dcs/test/tmp/distanceDf_$id")

        val partitions = distance.select("partition_id").distinct().collect().map(row => row.getAs[Int]("partition_id"))
        val distanceWithArray = mapping.keySet.foldLeft(distance)((df, column) =>
            df.withColumn(s"${column}_replaces",
                when(col(s"${column}_distance")(2) > 0, array(col(s"${column}_distance")(1))).otherwise(array())
            ))

        //todo: 因为yarn.nodemanager.local-dirs在/tem下shuffle write时会导致/挂载盘空间不够
        val iterator = partitions.iterator
        while (iterator.hasNext) {
            val partition = iterator.next()
            logger.info(s"begin $partition")
            //todo: 这儿filter掉了没有和prod匹配上的，如果逻辑需要保留全部这儿需要去除并且将这儿filter放在后面createReplaceLog的逻辑
            //            filterMin(distanceDf, mappingConfig)
            distanceWithArray.filter($"partition_id" === partition)
                    .filter("check_MOLE_NAME_CH != ''")
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
                                    if(isMore) colR._2 else colL._2
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
                    .write
                    .mode("append")
                    .parquet(path)
        }
    }

    def humanReplace(filterMinDistanceDf: DataFrame, mapping: Map[String, List[String]]): DataFrame = {
        val keys = List("MOLE_NAME", "PRODUCT_NAME", "SPEC", "DOSAGE", "PACK_QTY", "MANUFACTURER_NAME")
        val humanDf = spark.sql("select * from human_replace")

        val joinDf = filterMinDistanceDf
                .withColumn("in_min", concat(keys.map(x => col(s"in_$x")): _*))
                //                .withColumn("in_min", concat(col("in_MOLE_NAME") +: mapping.keys.toList.map(x => col(s"in_$x")): _*))
                .join(humanDf, $"in_min" === $"min", "left")
        mapping.keys.foldLeft(joinDf)((df, key) => {
            df.withColumn(s"${key}_distance", when($"min".isNull, col(s"${key}_distance"))
                    .otherwise(array(
                        col(s"${key}_distance")(0),
                        when(col(s"$key") === "", col(s"${key}_distance")(1)).otherwise(col(s"$key")),
                        when(col(s"${key}_distance")(0) === col(s"$key"), lit("0"))
                                .otherwise(when(col(s"$key") === "", col(s"${key}_distance")(2)).otherwise(lit("-1"))))
                    )
            )
        }).drop("in_min" +: humanDf.columns: _*)
    }

    private def getColumnDistance(df: DataFrame, column: String, checkColumns: List[String]): DataFrame = {
        val distanceUdf = udf((x: String, y: String) => BPEditDistance.getDistance(x, y))
        val min = udf((array: Seq[Seq[String]]) => BPEditDistance.minDistanceArray(array))
        df.na.fill("")
                .withColumn(s"${column}_distance", min(array(checkColumns.map(x =>
                    distanceUdf(col(s"in_$column"), col(s"check_$x"))): _*)))
    }

    private def replaceWithDistance(columnName: String, df: DataFrame): DataFrame = {
        val replaceUdf = udf((distance: Seq[String]) => BPEditDistance.replaceFunc(distance))
        df.withColumn(s"in_$columnName", replaceUdf(col(s"${columnName}_distance")))
    }

    private def createReplaceLog(replaceDf: DataFrame, inDfColumns: Array[String], mapping: Map[String, List[String]]): DataFrame = {
        replaceDf
                .selectExpr(Seq("id")
                        ++ mapping.keys.map(x => s"${x}_distance").toSeq
                        ++ mapping.keys.map(x => s"${x}_replaces").toSeq
                        ++ inDfColumns.map(x => s"in_$x").toSeq: _*
                )
                .withColumn("distances", map(mapping.keys.toSeq.flatMap(key => List(lit(key), col(s"${key}_distance"))): _*))
                .withColumn("replaces", map(mapping.keys.toSeq.flatMap(key => List(lit(key), col(s"${key}_replaces"))): _*))
                .withColumn("cols", array(inDfColumns.map(x => col(s"in_$x")).toSeq: _*))
                .select("id", "distances", "cols", "replaces")
                .flatMap {
                    case Row(id: Long, distances: Map[String, Seq[String]], cols: Seq[String], replaces: Map[String, Seq[String]]) =>
                        distances.map(x => (id, x._1, math.ceil(x._2.head.length / 5.0).toInt >= x._2(2).toInt, x._2.head, x._2(1), x._2(2).toInt, cols, replaces(x._1)))
                }
                .toDF("ID", "COL_NAME", "canReplace", "ORIGIN", "check", "distance", "cols", "CANDIDATE")
        //                .persist(StorageLevel.DISK_ONLY_2)
    }

    private def saveTable(filterMinDistanceDf: DataFrame, inDf: DataFrame, checkDf: DataFrame, mapping: Map[String, List[String]]): Unit = {
        val replaceLogDf = createReplaceLog(filterMinDistanceDf, inDf.columns, mapping).cache()
        val tableName = strategy.getJobConfig.getString(BPEditDistance.TABLE_NAME_CONFIG_KEY)
        val mode = "overwrite"
        val saveHandler = new TableSaveHandler(inDf, checkDf, tableName)
        saveHandler.saveReplaceTable(replaceLogDf, mode)
        //todo; 血缘
        //strategy.pushDataSet(s"${tableName}_replace", version, replaceUrl, "overwrite")
        saveHandler.saveNoReplaceTable(replaceLogDf, mode)
        //todo; 血缘
        //strategy.pushDataSet(s"${tableName}_no_replace", version, noReplaceUrl, "overwrite")
        saveHandler.saveNewTable(mapping.keys.foldLeft(filterMinDistanceDf)((df, s) => replaceWithDistance(s, df)), mode)
        //todo; 血缘
        //strategy.pushDataSet(s"${tableName}_new", version, noReplaceUrl, "overwrite")
    }

    override val description: String = "EditDistanceJob"

    class TableSaveHandler(inDf: DataFrame, checkDf: DataFrame, tableName: String) {
        lazy val inVersion: String = inDf.select("version").take(1).head.getAs[String]("version")
        lazy val checkVersion: String = checkDf.select("version").take(1).head.getAs[String]("version")

        def saveNewTable(df: DataFrame, mode: String): Unit = {
            val newTableName = s"${tableName}_new"
            val version = getVersion(newTableName, mode)
            df.selectExpr(Array("id", "check_PACK_ID as PACK_ID") ++ inDf.columns.map(x => s"in_$x as $x"): _*)
                    .withColumn("version", lit(version))
                    .write
                    .mode(mode)
                    .option("path", getTableSavePath(newTableName, inVersion, checkVersion, version))
                    .saveAsTable(newTableName)
        }

        def saveReplaceTable(df: DataFrame, mode: String): Unit = {
            val replaceTableName = s"${tableName}_replace"
            val version = getVersion(replaceTableName, mode)
            df.filter("canReplace = true and distance != 0")
                    .selectExpr(List("ID", "COL_NAME", "ORIGIN", "check as DEST") ++ inDf.columns.zipWithIndex.map(x => s"cols[${x._2}] as ORIGIN_${x._1}").toList: _*)
                    .withColumn("version", lit(version))
                    .write
                    .mode(mode)
                    .option("path", getTableSavePath(replaceTableName, inVersion, checkVersion, version))
                    .saveAsTable(replaceTableName)
        }

        def saveNoReplaceTable(df: DataFrame, mode: String): Unit = {
            val noReplaceLogTableName = s"${tableName}_no_replace"
            val version = getVersion(noReplaceLogTableName, mode)
            df.filter("canReplace = false")
                    .selectExpr(List("ID", "COL_NAME", "ORIGIN", "CANDIDATE", "DISTANCE")
                            ++ inDf.columns.zipWithIndex.map(x => s"cols[${x._2}] as ORIGIN_${x._1}").toList: _*)
                    .withColumn("version", lit(version))
                    .write
                    .mode(mode)
                    .option("path", getTableSavePath(noReplaceLogTableName, inVersion, checkVersion, version))
                    .saveAsTable(noReplaceLogTableName)
        }
    }

}

object BPEditDistance extends Serializable {
    final val TABLE_NAME_CONFIG_KEY = "tableName"
    final val TABLE_NAME_CONFIG_DOC = "need check table name"
    final val DATA_SETS_CONFIG_KEY = "dataSets"
    final val DATA_SETS_CONFIG_DOC = "dataSet ids"

    def checkColumnsFunc(map: Map[String, Int]): Boolean = {
        map.forall(r => (r._1.length / 5 + 1) >= r._2)
    }

    def replaceFunc(distance: Seq[String]): String = {
        if ((distance.head.length / 5 + 1) >= distance(2).toInt) distance(1) else distance.head
    }

    def minDistanceArray(array: Seq[Seq[String]]): Seq[String] = {
        array.minBy(x => x(2).toInt)
    }

    def getDistance(inputWord: String, targetWord: String): Array[String] = {
        val s1 = inputWord.replaceAll(" ", "").toUpperCase
        val s2 = targetWord.replaceAll(" ", "").toUpperCase
        val resContainer = Array.fill(s1.length + 1, s2.length + 1)(-1)
        val distanceNum = if (inputWord != targetWord && (s1 == s2 || checkSep(inputWord, targetWord))) -1 else distance(s1, s2, s1.length, s2.length, resContainer)
        Array(inputWord, targetWord, distanceNum.toString)
    }

    //    case class replaceLog(id: String, columnName: String, canReplace: Boolean, back: String, check: String, distance: Int)

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

