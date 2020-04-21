package com.pharbers.StreamEngine.Jobs.EditDistanceJob

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
    val jobId: String = strategy.getJobId
    val runId: String = strategy.getRunId
    override val id: String = jobId
    override val description: String = "human_replace_job"
    override val spark: SparkSession = strategy.getSpark

    import spark.implicits._

    val tableName = "human_replace"

    override def open(): Unit = super.open()

    override def exec(): Unit = {
    }

    //针对No_Replace_HBV 0228-Final
    def createHumanReplaceDf(df: DataFrame): Unit = {
        //todo:配置传入
        val tableVersion = getVersion(tableName)
        val table = spark.read.parquet(s"/common/public/human_replace/$tableVersion")
        val version = getVersion(tableName, "overwrite")
        val res = df.filter("PackID != '#N/A'")
                .na.fill("")
                .withColumn("ORIGIN_PRODUCT_NAME", when(col("ORIGIN_PRODUCT_NAME").isNotNull, col("ORIGIN_PRODUCT_NAME")).otherwise(lit("")))
                .withColumn("min", concat(col("ORIGIN_MOLE_NAME"), col("ORIGIN_PRODUCT_NAME"), col("ORIGIN_SPEC"), col("ORIGIN_DOSAGE"), col("ORIGIN_PACK_QTY"), col("ORIGIN_MANUFACTURER_NAME")))
                .selectExpr("min", "ORIGIN_MOLE_NAME as MOLE_NAME", "ORIGIN_PRODUCT_NAME1 as PRODUCT_NAME", "ORIGIN_SPEC1 as SPEC", "ORIGIN_DOSAGE1 as DOSAGE", "ORIGIN_PACK_QTY1 as PACK_QTY", "ORIGIN_MANUFACTURER_NAME1 as MANUFACTURER_NAME")
                .withColumn("version", lit(version))
                .distinct()
                .unionByName(table)
                .distinct()
        res.write
                .mode("overwrite")
                .option("path", s"/common/public/$tableName/$version")
                .saveAsTable(tableName)

    }

    //针对CPA_no_replace 第8次提数0417之前的
    def createHumanReplaceDfV2(df: DataFrame): Unit = {
        //todo:配置传入
        val mode = "append"
        val mappingConfig = List("MOLE_NAME", "PRODUCT_NAME", "SPEC", "DOSAGE", "PACK_QTY", "MANUFACTURER_NAME")
        val mapping = mappingConfig.map(x => x -> "").toMap
        val version = getVersion(tableName, mode)
        val resetOrigin = df.na.fill("na", Seq("REMARK")).filter("REMARK != 'na'")
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
                .write
                .mode(mode)
                .option("path", s"/common/public/human_replace/$version")
                .saveAsTable("human_replace")

    }

    def createHumanReplaceDfV3(df: DataFrame): Unit = {
        val minColumns = List("MOLE_NAME", "PRODUCT_NAME", "SPEC", "DOSAGE", "PACK_QTY", "MANUFACTURER_NAME")

        val minDf = df.na.fill("")
                .selectExpr(List("ORIGIN as ORI", "CANDIDATE as CAN", "COL_NAME as NAME") ::: minColumns.map(x => s"ORIGIN_$x as $x"): _*)
                .withColumn("CAN", regexp_replace(col("CAN"), "[\\[\\]\"]", ""))
                .withColumn("min", concat(minColumns.map(x => col(x)): _*))
                .withColumn("columns", map())

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
        val res = rules.foldLeft(minDf)((df, rule) => joinWithHumanRule(df, humanDf.filter($"REMARK" === rule), rule))
        res.write.parquet("/user/dcs/test/human_replace")

        val humanReplaceDf = spark.read.parquet("/user/dcs/test/human_replace")
        val tableVersion = getVersion(tableName)
        val oldTable = spark.read.parquet(s"/common/public/human_replace/$tableVersion")
                .withColumn("cols", map(minColumns.flatMap(x => List(lit(x), col(x))): _*))
                .selectExpr("min as oldMin", "cols")

        val margeMap = udf((map1: Map[String, String], map2: Map[String, String]) => map1 ++ map2.filter(x => x._2 != ""))
        val humanReplaceTable = humanReplaceDf.filter(size($"columns") > 0)
                .select("min", "columns")
                .groupBy("min").agg(first("columns") as "columns")
                .join(oldTable, $"min" === $"oldMin", "full")
                .withColumn("min", when($"min".isNull, $"oldMin") otherwise $"min")
                .withColumn("columns", when($"columns".isNull, $"cols") otherwise $"columns")
                .withColumn("columns", when($"cols".isNotNull, margeMap($"columns", $"cols")) otherwise $"columns")
                .selectExpr("min" +: minColumns.map(x => s"columns['$x'] as $x"): _*)
        val mode = "overwrite"
        val version = getVersion(tableName, mode)
        humanReplaceTable.withColumn("version", lit(version)).write
                .option("path", s"/common/public/human_replace/$version")
                .mode(mode)
                .saveAsTable(tableName)
    }

    def joinWithHumanRule(minDf: DataFrame, humanDf: DataFrame, rule: String): DataFrame ={
        val join: String => Column = s => s.split("=").head.split("\\+")
                .map(x => if(x == "ORIGIN") $"ORIGIN" === $"ORI" and $"CANDIDATE" === $"CAN" else col(x) === col(x.replace("ORIGIN_", "")))
                .foldLeft($"NAME" === $"COL_NAME")((l, r) => l and r)

        val selectCols = rule.split("=").head.split("\\+")
                .flatMap(x => if(x == "ORIGIN") List("ORIGIN", "CANDIDATE") else List(x))
        val humanDfFilter = humanDf.select("COL_NAME", selectCols: _*).distinct()
        val addMap= udf((map1: Map[String, String], key: String, value: String) => map1 ++ Map(key -> value))
        val res = minDf.join(humanDfFilter, join(rule), "left")
                .withColumn("columns", when($"COL_NAME".isNotNull, addMap($"columns", $"COL_NAME", $"CAN")) otherwise $"columns")
                .select(minDf.columns.map(x => col(x)): _*)
        res
    }

}

case class HumanReplaceRow(min: String, col_name: String, origin: String, cols: Map[String, String])

object TestHumanReplaceJob extends App {

    val localSpark = SparkSession.builder().config(new SparkConf().setMaster("local[8]")).enableHiveSupport().getOrCreate()
    localSpark.sparkContext.setLogLevel("INFO")
    val job = HumanReplaceJob(BPSComponentConfig("", "", Nil, Map()))

    val df = localSpark.read.format("csv")
            .option("header", "true")
            .load("file:///D:\\文件\\excel数据表\\产品\\humanReplace\\CPA_no_replace 第8次提数0417.csv")
    df.show()
//    job.createHumanReplaceDfV3(df)
    println("over")
    spark.close()
}
