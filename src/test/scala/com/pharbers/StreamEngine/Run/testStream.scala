package com.pharbers.StreamEngine.Run

import com.pharbers.StreamEngine.Utils.Component.ComponentContext
import com.pharbers.StreamEngine.Utils.Strategy.Schema.SchemaConverter
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor
import org.apache.spark.sql.functions.{from_json, lit, regexp_replace}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.scalatest.FunSuite

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/11/05 11:49
  * @note 一些值得注意的地方
  */
class testStream extends FunSuite  {
    test("test"){
        var totalRow: Long = 0
        val jobId = "64082-c50d-4ef0-bd98-cf88f0"
//        val spark = BPSparkSession(Map("log.level" -> "INFO"))
        val spark = BPSparkSession(null)
        import spark.spark.implicits._

//        val metaData = SchemaConverter.column2legalWithDF("MetaData",spark.sparkContext
//                .textFile("/workData/streamingV2/0829b025-48ac-450c-843c-6d4ee91765ca/metadata/bc6b6-3048-434a-a51a-a80c10")
//                .toDF("MetaData"))
//
//        if (metaData.count() > 0) {
//            val jobIdRow = metaData
//                    .withColumn("MetaData", lit(s"""{"jobId":"$jobId"}"""))
//
//            val traceIdRow = jobIdRow
//                    .withColumn("MetaData", lit(s"""{"traceId":"${jobId.substring(0, jobId.length - 1)}"}"""))
//
//            val metaDataStream = metaData.union(jobIdRow).union(traceIdRow).distinct()
//
//            val repMetaDataStream = metaData.head()
//                    .getAs[String]("MetaData")
//
//            metaDataStream.collect().foreach { x =>
//                val line = x.getAs[String]("MetaData")
//                if (line.contains("""{"length":""")) {
//                    totalRow = line.substring(line.indexOf(":") + 1)
//                            .replaceAll("_", "").replace("}", "").toLong
//                }
//            }

//            val schema = SchemaConverter.str2SqlType(repMetaDataStream)
//
//            val is = spark.readStream
//                    .schema(StructType(
//                        StructField("traceId", StringType) ::
//                                StructField("type", StringType) ::
//                                StructField("data", StringType) ::
//                                StructField("timestamp", TimestampType) ::
//                                StructField("jobId", StringType) :: Nil
//                    ))
//                    .parquet("/workData/streamingV2/0829b025-48ac-450c-843c-6d4ee91765ca/files")
//                    .filter($"jobId" === "bc6b6-3048-434a-a51a-a80c10" and $"type" === "SandBox")
//                    .withColumn("data", regexp_replace($"data", """\\"""", ""))
//                    .withColumn("data", regexp_replace($"data", " ", "_"))
//                    .withColumn("data", regexp_replace($"data", "\\(", ""))
//                    .withColumn("data", regexp_replace($"data", "\\)", ""))
//                    .withColumn("data", regexp_replace($"data", "=", ""))
//                    .withColumn("data", regexp_replace($"data", "\\\\n|\\\\\t", ""))
//                    .select(
//                        from_json($"data", schema).as("data")
//                    ).select("data.*")
//            val query = is.writeStream
//                    .outputMode("append")
//                    .format("parquet")
//                    					.format("console")
//                    .option("checkpointLocation", s"/test/dcs/$jobId/files/$jobId/checkpoint")
//                    .option("path", s"/test/dcs/$jobId/files/$jobId")
//                    .start()
//            while (true){
//                val cumulative = query.recentProgress.map(_.numInputRows).sum
//
//                if(query.lastProgress != null) {
//                    println("---->" + query.lastProgress.numInputRows)
//                }
//                println("=====>" + cumulative)
//                Thread.sleep(1000)
//            }
//        }
    }

    test("parquet stream"){
//        val spark = BPSparkSession(Map("log.level" -> "INFO"))
        val spark = BPSparkSession(null)
        val query = spark.readStream
                .schema("jobId string, YEAR string, MONTH string, HOSP_ID string, MOLE_NAME string, PRODUCT_NAME string, PACK_DES string, PACK_NUMBER string, VALUE string,STANDARD_UNIT string,DOSAGE string, DELIVERY_WAY string, CORP_NAME string")
                .parquet("/test/dcs/testLogs/testSink4/topics/source_d5b8c1ef5ab64b4b940c3f86fa960aee/partition=0")
                .writeStream.format("parquet") .option("checkpointLocation", s"/test/dcs/parquet/checkpoint")
                .option("path", s"/test/dcs/parquet/")
                .start()
        query.awaitTermination()
    }
}
