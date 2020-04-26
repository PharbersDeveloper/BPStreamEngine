package dcs.test

import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/01/03 16:18
  * @note 一些值得注意的地方
  */
object SparkSqlTest extends App {
    val spark = BPSparkSession(null)
    val df = spark.read.parquet("/common/public/cpa/0.0.11")
            .filter("company != 'Janssen'")
            .withColumn("version", lit("0.0.12"))

    val jassen = spark.read
            .format("csv")
            .option("header", true)
            .option("delimiter", ",")
            .load("/user/alex/jobs/bef4434e-a6b5-4680-b586-3d8a7db70f17/0058a115-8cdf-4395-8853-022a851ffdfe/contents",
                "/user/alex/jobs/bef4434e-a6b5-4680-b586-3d8a7db70f17/287a1f5b-eb6e-4d61-94c7-d72c14a7e681/contents")
            .withColumn("version", lit("0.0.12"))
            .withColumn("COMPANY", lit("Janssen"))
            .withColumn("SOURCE", lit("CPA&GYC"))


    df.unionByName(jassen)
            .repartition()
            .write
            .option("path", "/common/public/cpa/0.0.12")
            .saveAsTable("cpa")
}

object SparkSql extends App{
    val spark = SparkSession.builder().config(new SparkConf().setMaster("local[*]")).enableHiveSupport().getOrCreate()
    val df = spark.read
//            .format("csv")
//            .option("header", true)
//            .option("delimiter", ",")
//            .load("/jobs/a77038b2-5721-42bd-b9b4-07f07731dca6/6eeb4c6a-fb4f-498f-a763-864e64e324cf/contents")
            .parquet("/jobs/5e95b3801d45316c2831b98b/BPSOssPartitionJob/3f84542c-f23f-4d7b-836c-c8d656e287fa/contents")
    df.filter(col("jobId") === "").show(false)

}

object ReadParquet extends App{
//    val spark = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession]
    println(checkSep("10ml∶50mg", "50MG 10ML"))
    val spark = SparkSession.builder().config(new SparkConf().setMaster("local[*]")).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val checkFun = udf((s1: String, s2: String) => check(s1, s2))
    val df = spark.sql("SELECT distinct COL_NAME,  ORIGIN,  CANDIDATE,  ORIGIN_MOLE_NAME, ORIGIN_PRODUCT_NAME, ORIGIN_SPEC, ORIGIN_DOSAGE, ORIGIN_PACK_QTY, ORIGIN_MANUFACTURER_NAME, ORIGIN_MOLE_NAME FROM cpa_no_replace")
            .filter(checkFun(col("ORIGIN"), col("CANDIDATE")(0)))
    println(df.count())
//    df.show(false)

    def check(s1: String, s2: String): Boolean ={
//        checkUpperCase(s1, s2) ||
                checkSep(s1, s2)
    }

    def checkUpperCase(s1: String, s2: String): Boolean ={
        s1.replaceAll(" ",  "").toUpperCase() == s2.replaceAll(" ",  "").toUpperCase()
    }

    def checkSep(s1: String, s2: String): Boolean = {
        val list1 = s1.toUpperCase().split("[^A-Za-z0-9_\\u4e00-\\u9fa5]", -1).sorted
        val list2 = s2.toUpperCase().split("[^A-Za-z0-9_\\u4e00-\\u9fa5]", -1).sorted
        list1.sameElements(list2)
    }
}

