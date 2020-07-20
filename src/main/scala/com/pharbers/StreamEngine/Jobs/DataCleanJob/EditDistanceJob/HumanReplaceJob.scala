package com.pharbers.StreamEngine.Jobs.DataCleanJob.EditDistanceJob

import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Component2.{BPSComponentConfig, BPSConcertEntry}
import com.pharbers.StreamEngine.Utils.Job.{BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Utils.Strategy.JobStrategy.BPSCommonJobStrategy
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import org.apache.kafka.common.config.ConfigDef
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/** 功能描述
  *
  * @param config 构造参数
  * @author dcs
  * @version 0.0
  * @since 2020/03/03 10:05
  * @note 一些值得注意的地方
  */
object HumanReplaceJob {
    def apply(componentProperty: Component2.BPComponentConfig): HumanReplaceJob = new HumanReplaceJob(componentProperty)
}

class HumanReplaceJob(override val componentProperty: Component2.BPComponentConfig) extends BPStreamJob {

    override def createConfigDef(): ConfigDef = new ConfigDef()

    override type T = BPSCommonJobStrategy
    override val strategy: BPSCommonJobStrategy = BPSCommonJobStrategy(componentProperty, configDef)
    override val jobId: String = strategy.getJobId
    val runId: String = strategy.getRunId
    override val id: String = jobId
    override val description: String = "human_replace_job"
    override val spark: SparkSession = strategy.getSpark

    import spark.implicits._

    val tableName = "human_replace"
    val minColumns = List("MOLE_NAME", "PRODUCT_NAME", "SPEC", "DOSAGE", "PACK_QTY", "MANUFACTURER_NAME")

    override def open(): Unit = super.open()

    override def exec(): Unit = {
    }

    //针对No_Replace_HBV 0228-Final 和 CPA_no_replace 第9次提数0424
    def createHumanReplaceDf(df: DataFrame): DataFrame = {
        val humanReplaceDf = df.filter("PackID != '#N/A'")
                .na.fill("")
                .withColumn("ORIGIN_PRODUCT_NAME", when(col("ORIGIN_PRODUCT_NAME").isNotNull, col("ORIGIN_PRODUCT_NAME")).otherwise(lit("")))
                .withColumn("min", concat(col("ORIGIN_MOLE_NAME"), col("ORIGIN_PRODUCT_NAME"), col("ORIGIN_SPEC"), col("ORIGIN_DOSAGE"), col("ORIGIN_PACK_QTY"), col("ORIGIN_MANUFACTURER_NAME")))
                .selectExpr("min", "ORIGIN_MOLE_NAME as MOLE_NAME", "ORIGIN_PRODUCT_NAME1 as PRODUCT_NAME", "ORIGIN_SPEC1 as SPEC", "ORIGIN_DOSAGE1 as DOSAGE", "ORIGIN_PACK_QTY1 as PACK_QTY", "ORIGIN_MANUFACTURER_NAME1 as MANUFACTURER_NAME")
                .distinct()

        val oldTable = margeMinColumns(getOldTable)
        margeTable(margeMinColumns(humanReplaceDf), oldTable)
    }

    //针对CPA_no_replace 第8次提数0417之前的
    def createHumanReplaceDfV2(df: DataFrame): DataFrame = {
        //todo:配置传入
        val mode = "append"
        val mappingConfig = List("MOLE_NAME", "PRODUCT_NAME", "SPEC", "DOSAGE", "PACK_QTY", "MANUFACTURER_NAME")
        val mapping = mappingConfig.map(x => x -> "").toMap
        val version = getVersion(tableName, mode)
        val resetOrigin = df.na.fill("na", Seq("REMARK")).filter("REMARK != 'na'")
                .na.fill("")
                .withColumn("ORIGIN", regexp_replace(col("CANDIDATE"), "[\\[\\]\"]", ""))
                .withColumn("cols", map(lit("MOLE_NAME"), col("ORIGIN_MOLE_NAME"),
                    lit("PRODUCT_NAME"), col("ORIGIN_PRODUCT_NAME"),
                    lit("SPEC"), col("ORIGIN_SPEC"),
                    lit("DOSAGE"), col("ORIGIN_DOSAGE"),
                    lit("PACK_QTY"), col("ORIGIN_PACK_QTY"),
                    lit("MANUFACTURER_NAME"), col("ORIGIN_MANUFACTURER_NAME")))
                .withColumn("min", concat(mappingConfig.map(x => col(s"ORIGIN_$x")): _*))
                .selectExpr("min", "COL_NAME", "ORIGIN", "cols")
        val res = resetOrigin.as[HumanReplaceRow].groupByKey(x => x.min)
                .mapGroups((min, row) => {
                    val cols: Map[String, String] = mapping
                    val map = row.foldLeft(cols)((l, r) =>
                        l ++ Map(r.col_name -> r.origin))
                    (min, mappingConfig.map(x => map(x)))
                }).toDF("min", "cols")

        res.select(col("min") +: mappingConfig.zipWithIndex.map(x => expr(s"cols[${x._2}] as ${x._1}")): _*)
                .withColumn("version", lit(version))
                .distinct()

    }

    def createHumanReplaceDfV3(df: DataFrame): DataFrame = {
        val minColumns = List("MOLE_NAME", "PRODUCT_NAME", "SPEC", "DOSAGE", "PACK_QTY", "MANUFACTURER_NAME")

        val minDf = df.na.fill("")
                .selectExpr(List("ORIGIN as ORI", "CANDIDATE as CAN", "COL_NAME as NAME") ::: minColumns.map(x => s"ORIGIN_$x as $x"): _*)
                .withColumn("CAN", regexp_replace(col("CAN"), "[\\[\\]\"]", ""))
                .withColumn("min", concat(minColumns.map(x => col(x)): _*))
                .withColumn("cols", map())

        val humanDf = df.na.fill("na", Seq("REMARK")).filter("REMARK != 'na'")
                .withColumn("CANDIDATE", regexp_replace(col("CANDIDATE"), "[\\[\\]\"]", ""))

        val rules = List("ORIGIN=CANDIDATE",
                "ORIGIN+ORIGIN_MOLE_NAME+ORIGIN_PRODUCT_NAME+ORIGIN_DOSAGE=CANDIDATE",
                "ORIGIN+ORIGIN_MOLE_NAME+ORIGIN_DOSAGE+ORIGIN_MANUFACTURER_NAME=CANDIDATE",
                "ORIGIN+ORIGIN_MOLE_NAME+ORIGIN_PRODUCT_NAME=CANDIDATE",
                "ORIGIN+ORIGIN_PRODUCT_NAME=CANDIDATE",
                "ORIGIN+ORIGIN_MOLE_NAME+ORIGIN_PRODUCT_NAME+ORIGIN_MANUFACTURER_NAME=CANDIDATE",
                "ORIGIN+ORIGIN_MOLE_NAME=CANDIDATE",
                "ORIGIN+ORIGIN_MOLE_NAME+ORIGIN_SPEC+ORIGIN_DOSAGE+ORIGIN_MANUFACTURER_NAME=CANDIDATE",
                "ORIGIN+ORIGIN_MOLE_NAME+ORIGIN_MANUFACTURER_NAME=CANDIDATE",
                "ORIGIN+ORIGIN_MOLE_NAME+ORIGIN_DOSAGE=CANDIDATE",
                "ORIGIN_MOLE_NAME+ORIGIN_PRODUCT_NAME=CANDIDATE",
                "ORIGIN_PRODUCT_NAME=CANDIDATE")
        val humanReplaceDf = rules.foldLeft(minDf)((df, rule) => joinWithHumanRule(df, humanDf.filter($"REMARK" === rule), rule))

        val oldTable = margeMinColumns(getOldTable)

        val humanReplaceTable = margeTable(humanReplaceDf, oldTable)
        humanReplaceTable
    }

    def createHumanReplaceDfV4(df: DataFrame): DataFrame = {
        val prod = spark.sql("select cast(PACK_ID as int) as PACK_ID, MOLE_NAME_CH, PROD_NAME_CH, MNF_NAME_CH, DOSAGE, SPEC, PACK from prod")
        val humanReplaceDf = df.filter("PACK_ID_HUMAN != 0")
                .na.fill("")
                .withColumn("ORIGIN_PRODUCT_NAME", when(col("ORIGIN_PRODUCT_NAME").isNotNull, col("ORIGIN_PRODUCT_NAME")).otherwise(lit("")))
                .withColumn("min", concat(col("ORIGIN_MOLE_NAME"), col("ORIGIN_PRODUCT_NAME"), col("ORIGIN_SPEC"), col("ORIGIN_DOSAGE"), col("ORIGIN_PACK_QTY"), col("ORIGIN_MANUFACTURER_NAME")))
                .select("min", "PACK_ID_HUMAN")
                .join(prod, $"PACK_ID_HUMAN" === $"PACK_ID")
                .selectExpr("min", "MOLE_NAME_CH as MOLE_NAME", "PROD_NAME_CH as PRODUCT_NAME", "SPEC as SPEC", "DOSAGE as DOSAGE", "PACK as PACK_QTY", "MNF_NAME_CH as MANUFACTURER_NAME")
                .distinct()

        val oldTable = margeMinColumns(getOldTable)
        margeTable(margeMinColumns(humanReplaceDf), oldTable)
    }

    def saveTable(humanReplaceTable: DataFrame): Unit ={
        val mode = "overwrite"
        val version = getVersion(tableName, mode)
        humanReplaceTable.withColumn("version", lit(version)).write
                .option("path", s"s3a://ph-stream/common/public/human_replace/$version")
                .mode(mode)
                .saveAsTable(tableName)
    }

    def getOldTable: DataFrame = {
        val tableVersion = getVersion(tableName)
        spark.read.parquet(s"s3a://ph-stream/common/public/human_replace/$tableVersion")
    }

    def margeMinColumns(df: DataFrame): DataFrame = {
        df.withColumn("cols", map(minColumns.flatMap(x => List(lit(x), col(x))): _*))
    }

    def margeTable(humanReplaceDf: DataFrame, oldTable: DataFrame): DataFrame ={
        val margeMap = udf((map1: Map[String, String], map2: Map[String, String]) => map2.filter(x => x._2 != "") ++ map1)
        humanReplaceDf.selectExpr("min", "cols as columns")
                .filter(size($"columns") > 0)
                .select("min", "columns")
                .groupBy("min").agg(first("columns") as "columns")
                .join(oldTable.selectExpr("min as oldMin", "cols"), $"min" === $"oldMin", "full")
                .withColumn("min", when($"min".isNull, $"oldMin") otherwise $"min")
                .withColumn("columns", when($"columns".isNull, $"cols") otherwise $"columns")
                .withColumn("columns", when($"cols".isNotNull, margeMap($"columns", $"cols")) otherwise $"columns")
                .selectExpr("min" +: minColumns.map(x => s"columns['$x'] as $x"): _*)
                .na.fill("")

    }

    private def joinWithHumanRule(minDf: DataFrame, humanDf: DataFrame, rule: String): DataFrame ={
        val join: String => Column = s => s.split("=").head.split("\\+")
                .map(x => if(x == "ORIGIN") $"ORIGIN" === $"ORI" and $"CANDIDATE" === $"CAN" else col(x) === col(x.replace("ORIGIN_", "")))
                .foldLeft($"NAME" === $"COL_NAME")((l, r) => l and r)

        val selectCols = rule.split("=").head.split("\\+")
                .flatMap(x => if(x == "ORIGIN") List("ORIGIN", "CANDIDATE") else List(x))
        val humanDfFilter = humanDf.select("COL_NAME", selectCols: _*).distinct()
        val addMap= udf((map1: Map[String, String], key: String, value: String) => map1 ++ Map(key -> value))
        val res = minDf.join(humanDfFilter, join(rule), "left")
                .withColumn("cols", when($"COL_NAME".isNotNull, addMap($"cols", $"COL_NAME", $"CAN")) otherwise $"cols")
                .select(minDf.columns.map(x => col(x)): _*)
        res
    }
}

case class HumanReplaceRow(min: String, col_name: String, origin: String, cols: Map[String, String])