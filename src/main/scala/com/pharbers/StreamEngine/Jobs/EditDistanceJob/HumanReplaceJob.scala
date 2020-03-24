package com.pharbers.StreamEngine.Jobs.EditDistanceJob

import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSCommonJoBStrategy
import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}
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
case class HumanReplaceJob(jobContainer: BPSJobContainer, spark: SparkSession, config_map: Map[String, String]) extends BPStreamJob {

    import spark.implicits._

    override val componentProperty: Component2.BPComponentConfig = null
    override def createConfigDef(): ConfigDef = ???

    override type T = BPSCommonJoBStrategy
    override val strategy: BPSCommonJoBStrategy = BPSCommonJoBStrategy(config_map)
    val jobId: String = strategy.getJobId
    val runId: String = strategy.getRunId
    override val id: String = jobId

    implicit val mappingConfig = List("MOLE_NAME", "PRODUCT_NAME", "SPEC", "DOSAGE", "PACK_QTY", "MANUFACTURER_NAME")

    override def open(): Unit = super.open()

    override def exec(): Unit = {

    }

    def createHumanReplaceDf(df: DataFrame): Unit = {
        //todo:配置传入
        val version = "0.0.1"
        val table = spark.sql("select * from human_replace")
        df.filter("PackID != '#N/A'")
                .na.fill("")
                .withColumn("ORIGIN_PRODUCT_NAME", when(col("ORIGIN_PRODUCT_NAME").isNotNull, col("ORIGIN_PRODUCT_NAME")).otherwise(lit("")))
//                .select("ORIGIN_MOLE_NAME", "ORIGIN_PRODUCT_NAME", "ORIGIN_PRODUCT_NAME2", "ORIGIN_SPEC", "ORIGIN_SPEC2", "ORIGIN_DOSAGE", "ORIGIN_DOSAGE2", "ORIGIN_PACK_QTY", "ORIGIN_PACK_QTY", "ORIGIN_MANUFACTURER_NAME", "ORIGIN_MANUFACTURER_NAME2")
                .withColumn("min", concat(col("ORIGIN_MOLE_NAME"), col("ORIGIN_PRODUCT_NAME"), col("ORIGIN_SPEC"), col("ORIGIN_DOSAGE"), col("ORIGIN_PACK_QTY"), col("ORIGIN_MANUFACTURER_NAME")))
                .selectExpr("min", "ORIGIN_MOLE_NAME as MOLE_NAME", "ORIGIN_PRODUCT_NAME1 as PRODUCT_NAME", "ORIGIN_SPEC1 as SPEC", "ORIGIN_DOSAGE1 as DOSAGE", "ORIGIN_PACK_QTY1 as PACK_QTY", "ORIGIN_MANUFACTURER_NAME1 as MANUFACTURER_NAME")
                .distinct()
                .unionByName(table)
                .distinct()
                .write
                .mode("overwrite")
                .option("path", s"/common/public/human_replace/$version")
                .saveAsTable("human_replace")

    }
}

object TestHumanReplaceJob extends App {

    import com.pharbers.StreamEngine.Utils.Session.Spark.BPSparkSession

//    val spark = BPSparkSession()
//    spark.sparkContext.setLogLevel("INFO")
    val job = HumanReplaceJob(null, null, Map("jobId" -> "test_0228", "runId" -> "test_0228"))

    val localSpark = SparkSession.builder().config(new SparkConf().setMaster("local[*]")).enableHiveSupport().getOrCreate()
    val df = localSpark.read.format("csv")
            .option("header", "true")
            .load("file:///D:\\文件\\excel数据表\\产品\\humanReplace\\No_Replace_HBV 0228-Final.csv")
    job.createHumanReplaceDf(df)
}
