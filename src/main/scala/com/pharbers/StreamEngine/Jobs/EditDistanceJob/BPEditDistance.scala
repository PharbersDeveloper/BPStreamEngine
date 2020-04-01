package com.pharbers.StreamEngine.Jobs.EditDistanceJob

import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSDataMartJobStrategy
import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import com.pharbers.StreamEngine.Jobs.EditDistanceJob.BPEditDistance.{TABLE_NAME_CONFIG_DOC, TABLE_NAME_CONFIG_KEY, DATA_SETS_CONFIG_KEY, DATA_SETS_CONFIG_DOC}

/** 功能描述
  *
  * @author dcs
  * @version 0.0
  * @since 2020/02/04 13:38
  * @note 一些值得注意的地方
  */
case class BPEditDistance(jobContainer: BPSJobContainer, spark: SparkSession, config: Map[String, String]) extends BPStreamJob {

    import spark.implicits._

    val configDef: ConfigDef = new ConfigDef()
            .define(TABLE_NAME_CONFIG_KEY, Type.STRING, "cpa", Importance.HIGH, TABLE_NAME_CONFIG_DOC)
            .define(DATA_SETS_CONFIG_KEY, Type.LIST, Importance.HIGH, DATA_SETS_CONFIG_DOC)
    override type T = BPSDataMartJobStrategy
    override val strategy: BPSDataMartJobStrategy = new BPSDataMartJobStrategy(config, configDef)
    val jobId: String = strategy.getJobId
    val runId: String = strategy.getRunId
    override val id: String = jobId
    //todo: 配置传入
    implicit val mappingConfig: Map[String, List[String]] = Map(
        "PRODUCT_NAME" -> List("PROD_NAME_CH"),
        "SPEC" -> List("SPEC"),
        "DOSAGE" -> List("DOSAGE"),
        "PACK_QTY" -> List("PACK"),
        "MANUFACTURER_NAME" -> List("CORP_NAME_CH", "CORP_NAME_EN", "MNF_NAME_CH", "MNF_NAME_EN")
    )

    override def open(): Unit = {
//                inputStream = Some(spark.sql("select * from cpa"))
        inputStream = jobContainer.inputStream
    }


    override def exec(): Unit = {
        //todo: 配置传入
        val mappingDf = spark.sql("select * from prod")
        val tableName = strategy.getJobConfig.getString(BPEditDistance.TABLE_NAME_CONFIG_KEY)
        inputStream match {
            case Some(in) => check(in.repartition(2000), mappingDf, tableName)
            case _ =>
        }
    }

    override def close(): Unit = {
        super.close()
        jobContainer.finishJobWithId(id)
    }

    def check(in: DataFrame, checkDf: DataFrame, tableName: String): Unit = {
        val mapping = Map() ++ mappingConfig
        //        todo: 配置传入
//        val repartitionByIdNum = 50
//        val inDfRename = in.columns.foldLeft(in)((l, r) =>
//            l.withColumnRenamed(r, s"in_$r"))
//        val checkDfRename = checkDf.columns.foldLeft(checkDf)((l, r) =>
//            l.withColumnRenamed(r, s"check_$r"))
//
//        val joinDf = inDfRename
//                .withColumn("id", monotonically_increasing_id())
//                .withColumn("partition_id", $"id" % repartitionByIdNum)
//                .join(checkDfRename, col("in_MOLE_NAME") === col("check_MOLE_NAME_CH"), "left")
//                .na.fill("")
//
//        val distanceDfTmp = mapping.foldLeft(joinDf)((l, r) => getColumnDistance(l, r._1, r._2))
        //                .persist(StorageLevel.DISK_ONLY_2)

//        distanceDfTmp.write
//                .partitionBy("partition_id")
//                .mode("overwrite")
//                .parquet(s"/user/dcs/test/tmp/distanceDf_$id")

        //同一个id取编辑距离和最小的一行

        val distance = spark.read.parquet(s"/user/dcs/test/tmp/distanceDf_$id")

        val partitions = distance.select("partition_id").distinct().collect().map(row => row.getAs[Int]("partition_id"))

        partitions.foreach(partition => {
            logger.info(s"begin $partition")
            val distanceDf = distance.filter($"partition_id" === partition)
            val filterMinDistanceDf = filterMinDistance(distanceDf)

            //todo: 这儿正式运行后会根据每次id不同生成不同的文件，所以需要定期删除
            //使用人工干预表处理
            filterMinDistanceDf.write
                    .mode("append")
                    .parquet(s"/user/dcs/test/tmp/res_$id")
        })

        val humanReplaceDf = spark.sql("select * from human_replace")

        val filterMinDistanceDf = humanReplace(spark.read.parquet(s"/user/dcs/test/tmp/res_$id").repartitionByRange($"in_MOLE_NAME"), humanReplaceDf)

        val cpaVersion = in.select("version").take(1).head.getAs[String]("version")
        val prodVersion = checkDf.select("version").take(1).head.getAs[String]("version")
        val tables = spark.sql("show tables").select("tableName").collect().map(x => x.getString(0))
        val version = if (tables.contains(s"${tableName}_new")) {
            val old = spark.sql(s"select version from $tableName limit 1").take(1).head.getString(0).split("\\.")
            s"${old.head}.${old(1)}.${old(2).toInt + 1}"
        } else {
            "0.0.1"
        }
        val intColumnsExpr = in.columns.map(x => s"in_$x as $x").toList

        val replaceLogDf = createReplaceLog(filterMinDistanceDf, in.columns)
        val replaceUrl = s"/common/public/${tableName}_replace/${tableName}_${cpaVersion}_${prodVersion}_replace/$version"
        replaceLogDf.filter("canReplace = true and distance != 0")
                .selectExpr(List("ID", "COL_NAME", "ORIGIN", "check as DEST") ++ in.columns.zipWithIndex.map(x => s"cols[${x._2}] as ORIGIN_${x._1}").toList: _*)
                .write
                //                .bucketBy(11, "ORIGIN_MOLE_NAME")
//                .partitionBy("ORIGIN_MOLE_NAME")
                .mode("overwrite")
                .option("path", replaceUrl)
                .saveAsTable(s"${tableName}_replace")
        strategy.pushDataSet(s"${tableName}_replace", version, replaceUrl, "overwrite")

        val noReplaceUrl = s"/common/public/${tableName}_no_replace/${tableName}_${cpaVersion}_${prodVersion}_no_replace/$version"
        replaceLogDf.filter("canReplace = false")
                .selectExpr(List("ID", "COL_NAME", "ORIGIN", "check as CANDIDATE", "DISTANCE") ++ in.columns.zipWithIndex.map(x => s"cols[${x._2}] as ORIGIN_${x._1}").toList: _*)
                .withColumn("CANDIDATE", array($"CANDIDATE"))
                .write
                //                .bucketBy(11, "ORIGIN_MOLE_NAME")
//                .partitionBy("ORIGIN_MOLE_NAME")
                .mode("overwrite")
                .option("path", noReplaceUrl)
                .saveAsTable(s"${tableName}_no_replace")

        strategy.pushDataSet(s"${tableName}_no_replace", version, noReplaceUrl, "overwrite")

        val res = mapping.keys.foldLeft(filterMinDistanceDf)((df, s) => replaceWithDistance(s, df))
        val newCpaUrl = s"/common/public/${tableName}_new/${tableName}_${cpaVersion}_${prodVersion}_new/$version"
        res.selectExpr(List("id", "check_PACK_ID as PACK_ID") ::: intColumnsExpr: _*)
                .write
                .mode("overwrite")
                .option("path", newCpaUrl)
                .saveAsTable(s"${tableName}_new")
//        strategy.pushDataSet(s"${tableName}_new", version, noReplaceUrl, "overwrite")
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

    private def filterMinDistance(distanceDf: DataFrame): DataFrame = {
        val mapping = Map() ++ mappingConfig
        //todo: 这儿filter掉了没有和prod匹配上的，如果逻辑需要保留全部这儿需要去除并且将这儿filter放在后面createReplaceLog的逻辑
        distanceDf.filter("check_MOLE_NAME_CH != ''")
                .groupByKey(x => x.getAs[Long]("id"))
                .mapGroups((_, row) => row.reduce((l, r) => {
                    val func: Row => Int = row => mapping.keySet.foldLeft(0)((x, y) => x + row.getAs[Seq[String]](s"${y}_distance")(2).toInt)
                    val leftSum = func(l)
                    val rightSum = func(r)
                    if (leftSum > rightSum) r else l
                }))(RowEncoder(distanceDf.schema))
        //                .persist(StorageLevel.DISK_ONLY_2)
    }

    private def createReplaceLog(replaceDf: DataFrame, inDfColumns: Array[String])(implicit mapping: Map[String, List[String]]): DataFrame = {
        replaceDf.selectExpr(Seq("id") ++ mapping.keys.map(x => s"${x}_distance").toSeq ++ inDfColumns.map(x => s"in_$x").toSeq: _*)
                .withColumn("distances", map(mapping.keys.toSeq.flatMap(key => List(lit(key), col(s"${key}_distance"))): _*))
                .withColumn("cols", array(inDfColumns.map(x => col(s"in_$x")).toSeq: _*))
                .select("id", "distances", "cols")
                .flatMap {
                    case Row(id: Long, distances: Map[String, Seq[String]], cols: Seq[String]) =>
                        distances.map(x => (id, x._1, math.ceil(x._2.head.length / 5.0).toInt >= x._2(2).toInt, x._2.head, x._2(1), x._2(2).toInt, cols))
                }
                .toDF("ID", "COL_NAME", "canReplace", "ORIGIN", "check", "distance", "cols")
        //                .persist(StorageLevel.DISK_ONLY_2)
    }

    private def humanReplace(filterMinDistanceDf: DataFrame, humanDf: DataFrame)(implicit mapping: Map[String, List[String]]): DataFrame = {
        val keys = List("MOLE_NAME", "PRODUCT_NAME", "SPEC", "DOSAGE", "PACK_QTY", "MANUFACTURER_NAME")
        val joinDf = filterMinDistanceDf
                .withColumn("in_min", concat(keys.map(x => col(s"in_$x")): _*))
                //                .withColumn("in_min", concat(col("in_MOLE_NAME") +: mapping.keys.toList.map(x => col(s"in_$x")): _*))
                .join(humanDf, $"in_min" === $"min", "left")
        mapping.keys.foldLeft(joinDf)((df, key) => {
            df.withColumn(s"${key}_distance", when($"min".isNull, col(s"${key}_distance"))
                    .otherwise(array(
                        col(s"${key}_distance")(0),
                        col(s"$key"),
                        when(col(s"${key}_distance")(0) === col(s"$key"), lit("0")).otherwise(lit("-1")))
                    )
            )
        }).drop("in_min" +: humanDf.columns: _*)
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
        val resContainer = Array.fill(inputWord.length + 1, targetWord.length + 1)(-1)
        Array(inputWord, targetWord, distance(inputWord, targetWord, inputWord.length, targetWord.length, resContainer).toString)
    }

    case class replaceLog(id: String, columnName: String, canReplace: Boolean, back: String, check: String, distance: Int)

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

