package com.pharbers.StreamEngine.Jobs.DataCleanJob.test

import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import org.apache.spark.sql.functions.{col, split, lit}

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/07/09 10:43
  * @note 一些值得注意的地方
  */
object MaxResultJob extends App {
    val spark = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession]
    val maxDf = spark.read.parquet("s3a://ph-stream/common/public/max_result/0.0.1")
    val hosp = spark.sql("select PREFECTURE as Prefecture, CITY_TIER_2018 as Tier, REGION as Region, PHA_ID from hosp")
    val prod = spark.sql("select concat_ws('|', PROD_NAME_CH, DOSAGE, SPEC, PACK, MNF_NAME_CH) as min, PACK_ID from prod")
    val maxDfWithRegionAndProd = maxDf.select("PHA", "Prod_Name").distinct()
            .join(hosp, col("PHA") === col("PHA_ID"), "left")
            .drop("PHA_ID")
            .join(prod, col("Prod_Name") === col("min"), "left")
            .drop("min")
            .withColumn("Product_Name", split(col("Prod_Name"), "\\|")(0))
            .withColumn("Dosage", split(col("Prod_Name"), "\\|")(1))
            .withColumn("Spec", split(col("Prod_Name"), "\\|")(2))
            .withColumn("Pack", split(col("Prod_Name"), "\\|")(3))
            .withColumn("MNF_Name", split(col("Prod_Name"), "\\|")(4))

    maxDfWithRegionAndProd
            .join(maxDf, maxDfWithRegionAndProd("Prod_Name") === maxDf("Prod_Name") and maxDfWithRegionAndProd("PHA") === maxDf("PHA"))
            .drop(maxDfWithRegionAndProd("Prod_Name"))
            .drop(maxDfWithRegionAndProd("PHA"))
            .withColumn("version", lit("0.0.2"))
            .write.mode("overwrite")
            .option("path", "s3a://ph-stream/common/public/max_result/0.0.2")
            .saveAsTable("max_result")
}
