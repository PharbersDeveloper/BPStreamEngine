package com.pharbers.StreamEngine.Jobs.EditDistanceJob

import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Job.{BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Utils.Strategy.BPSCommonJobStrategy
import org.apache.kafka.common.config.ConfigDef
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.encoders.RowEncoder
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
case class HumanReplaceJob(jobContainer: BPSJobContainer, spark: SparkSession, config: Map[String, String]) extends BPStreamJob {

    import spark.implicits._

    override val componentProperty: Component2.BPComponentConfig = null
    override def createConfigDef(): ConfigDef = ???

    override type T = BPSCommonJobStrategy
    override val strategy: BPSCommonJobStrategy = BPSCommonJobStrategy(config_map)
    val jobId: String = strategy.getJobId
    val runId: String = strategy.getRunId
    override val id: String = jobId
    override val description: String = "human_replace_job"

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

    def createHumanReplaceDfV2(df: DataFrame): Unit = {
        //todo:配置传入
        val mappingConfig = List("MOLE_NAME", "PRODUCT_NAME", "SPEC", "DOSAGE", "PACK_QTY", "MANUFACTURER_NAME")
        val version = "0.0.2"
        val resetOrigin = df.na.fill("na", Seq("REMARK")).filter("REMARK != 'na'")
                .withColumn("ORIGIN", when(col("REMARK") === "ORIGIN=CANDIDATE", regexp_replace(col("CANDIDATE"), "[\\[\\]\"]", "")).otherwise(concat(col("ORIGIN"), col("ORIGIN_MANUFACTURER_NAME"))))
                .withColumn("cols", map(lit("MOLE_NAME"), col("ORIGIN_MOLE_NAME"),
                    lit("PRODUCT_NAME"), col("ORIGIN_PRODUCT_NAME"),
                    lit("SPEC"), col("ORIGIN_SPEC"),
                    lit("DOSAGE"), col("ORIGIN_DOSAGE"),
                    lit("PACK_QTY"), col("ORIGIN_PACK_QTY"),
                    lit("MANUFACTURER_NAME"), col("ORIGIN_MANUFACTURER_NAME")))
                .selectExpr("ID", "COL_NAME", "ORIGIN", "cols")
        val res = resetOrigin.as[HumanReplaceRow].groupByKey(x => x.id)
                .mapGroups((_, row) => {
                    val cols = row.next().cols
                    val map = row.foldLeft(cols)((l, r) =>
                        l ++ Map(r.col_name -> r.origin))
                    (mappingConfig.map(x => cols(x)).mkString(""), mappingConfig.map(x => map(x)))
                }).toDF("min", "cols")

        res.select(col("min") +: mappingConfig.zipWithIndex.map(x => expr(s"cols[${x._2}] as ${x._1}")): _*)
//                .unionByName(table)
                .distinct()
                .write
                .mode("append")
                .option("path", s"/common/public/human_replace/$version")
                .saveAsTable("human_replace")

    }
}

case class HumanReplaceRow(id: String, col_name: String, origin: String, cols: Map[String, String])

object TestHumanReplaceJob extends App {

    import com.pharbers.StreamEngine.Utils.Session.Spark.BPSparkSession
    val localSpark = SparkSession.builder().config(new SparkConf().setMaster("local[*]")).enableHiveSupport().getOrCreate()

//        val spark = BPSparkSession()
//        spark.sparkContext.setLogLevel("INFO")
    val job = HumanReplaceJob(null, localSpark, Map("jobId" -> "test_0228", "runId" -> "test_0228"))

    val df = localSpark.read.format("csv")
            .option("header", "true")
            .load("file:///D:\\文件\\excel数据表\\产品\\humanReplace\\CPA_no_replace 提数20200320.csv")
    job.createHumanReplaceDfV2(df)
}
