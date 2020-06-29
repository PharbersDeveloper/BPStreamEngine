package com.pharbers.StreamEngine.Jobs.MartDataCheckJob

import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import org.apache.spark.sql.functions.{col, concat, lit}

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/06/18 18:05
  * @note 一些值得注意的地方
  */
object HumanReplaceCheck extends App {
    val spark = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession]
    import spark.spark.implicits._
    //    spark.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    //    spark.sparkContext.setLogLevel("WARN")
    val minKeys = List("MOLE_NAME", "PRODUCT_NAME", "SPEC", "DOSAGE", "PACK_QTY", "MANUFACTURER_NAME")
    val cpa = spark.sql("select * from cpa").withColumn("cpa_min", concat(minKeys.map(col): _*))
    val cpaOld = spark.read.parquet("s3a://ph-stream/common/public/cpa/0.0.13")
            .selectExpr(minKeys.map(x => s"$x as cpa_$x"): _*)
            .withColumn("cpa_min", concat(minKeys.map(x => col( s"cpa_$x")): _*))
            .distinct()

    val human = spark.sql("select * from human_replace")
    val canMapping = human
            .join(cpa.select("cpa_min").distinct(), $"min" === $"cpa_min")
            .drop("cpa_min")
            .cache()
    val notMapping = human
            .join(cpa.select("cpa_min").distinct(), $"min" === $"cpa_min", "left")
            .filter("cpa_min is null")
            .drop("cpa_min")
            .cache()

    println(s"can ${canMapping.count()}")
    println(s"no ${notMapping.count()}")
    val replacePack = notMapping.join(cpaOld, col("cpa_min") === col("min"))
            .withColumn("cpa_PACK_QTY", col("PACK_QTY"))
            .withColumn("min", concat(minKeys.map(x => col( s"cpa_$x")): _*))
            .select("min", minKeys: _*)
            .distinct()
            .cache()
    println(replacePack.count())
    replacePack.unionByName(canMapping.select("min", minKeys: _*))
            .withColumn("version", lit("0.0.1"))
            .write.mode("overwrite")
            .option("path", "s3a://ph-stream/common/public/human_replace_new/0.0.1").saveAsTable("human_replace_new")
}
