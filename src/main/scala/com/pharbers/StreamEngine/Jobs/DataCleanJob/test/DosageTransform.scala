package com.pharbers.StreamEngine.Jobs.DataCleanJob.test

import com.pharbers.StreamEngine.Jobs.DataCleanJob.test.SpecUnitTransform.df
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import org.apache.spark.sql.functions._

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/06/22 13:23
  * @note 一些值得注意的地方
  */
object DosageTransform extends App {
    val spark = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession].spark
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    val tableName = "pfizer_test"

    val containsUDF = udf((s1: String, s2: Seq[String]) => contains(s1, s2))

    val df = spark.sql(s"SELECT distinct COL_NAME,  ORIGIN,  CANDIDATE,  ORIGIN_MOLE_NAME, ORIGIN_PRODUCT_NAME, ORIGIN_SPEC, ORIGIN_DOSAGE, ORIGIN_PACK_QTY, ORIGIN_MANUFACTURER_NAME FROM ${tableName}_no_replace")
            .filter("COL_NAME == 'DOSAGE'")
            .withColumn("can_replace", containsUDF($"ORIGIN", $"CANDIDATE"))

    df.write.mode("overwrite").option("path", "s3a://ph-stream/common/public/dosage_contains_transform").saveAsTable("dosage_contains_transform")


    def contains(s1: String, s2: Seq[String]): String ={
        s2.find(x => s1.contains(x)).getOrElse("")
    }
}
