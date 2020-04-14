package com.pharbers.StreamEngine.Jobs.EditDistanceJob

import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Component2.{BPSComponentConfig, BPSConcertEntry}
import com.pharbers.StreamEngine.Utils.Job.{BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Utils.Strategy.JobStrategy.BPSCommonJobStrategy
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import org.apache.kafka.common.config.ConfigDef
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
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

}

case class HumanReplaceRow(min: String, col_name: String, origin: String, cols: Map[String, String])

object TestHumanReplaceJob extends App {

    val localSpark = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession]

    val job = HumanReplaceJob(BPSComponentConfig("", "", Nil, Map()))

    val df = localSpark.read.format("csv")
            .option("header", "true")
            .load("file:///D:\\文件\\excel数据表\\产品\\humanReplace\\CPA_no_replace 第五次提数0413.csv")
    job.createHumanReplaceDfV2(df)
    println("over")
    spark.close()
}
