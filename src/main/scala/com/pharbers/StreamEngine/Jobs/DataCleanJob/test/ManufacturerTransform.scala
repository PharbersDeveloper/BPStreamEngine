package com.pharbers.StreamEngine.Jobs.DataCleanJob.test

import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import org.apache.spark.sql.functions.udf

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/06/22 15:05
  * @note 一些值得注意的地方
  */
object ManufacturerTransform extends App {
    val spark = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession].spark
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    val tableName = "pfizer_test"

    val replaceAndContainsUDF = udf((s1: String, s2: Seq[String]) => replaceAndContains(s1, s2))

    val df = spark.sql(s"SELECT distinct COL_NAME,  ORIGIN,  CANDIDATE,  ORIGIN_MOLE_NAME, ORIGIN_PRODUCT_NAME, ORIGIN_SPEC, ORIGIN_DOSAGE, ORIGIN_PACK_QTY, ORIGIN_MANUFACTURER_NAME FROM ${tableName}_no_replace")
            .filter("COL_NAME == 'MANUFACTURER_NAME'")
            .withColumn("can_replace", replaceAndContainsUDF($"ORIGIN", $"CANDIDATE"))

    df.write.mode("overwrite").option("path", "s3a://ph-stream/common/public/manufacturer_transform").saveAsTable("manufacturer_transform")


    def replaceAndContains(s1: String, s2: Seq[String]): String ={
        val list = List(
            "股份", "有限", "公司", "集团", "制药", "厂", "药业", "责任", "健康", "科技", "生物", "工业"
        )
        val manufacturer1 = list.foldLeft(s1)((s, r) => s.replaceAll(r, ""))
        s2.find(x => {
            val manufacturer2 = list.foldLeft(x)((s, r) => s.replaceAll(r, ""))
            manufacturer1.contains(manufacturer2) || manufacturer2.contains(manufacturer1)
        }).getOrElse("")
    }
}
