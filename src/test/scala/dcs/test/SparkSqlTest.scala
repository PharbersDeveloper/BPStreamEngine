package dcs.test

import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import org.apache.spark.sql.functions.lit

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
    val spark = BPSparkSession(null)
    val df = spark.read.format("csv")
            .option("header", true)
            .option("delimiter", ",")
            .load("/jobs/a77038b2-5721-42bd-b9b4-07f07731dca6/6eeb4c6a-fb4f-498f-a763-864e64e324cf/contents")
    df.write.option("path", "/common/public/prod/0.0.1")
            .saveAsTable("prod")
}

object ReadParquet extends App{
    val spark = BPSparkSession(null)
    val df = spark.read.parquet("/jobs/f9b76128-8019-4d1a-bf6c-b12b683f778c/c2ed46c4-c8b8-488f-944e-29c39b695e43/contents/part-00000-604be270-2ee4-400a-91d9-9309fce61454-c000.snappy.parquet")
    println(df.count())
}
