package com.pharbers.StreamEngine.Others.clock

import com.pharbers.StreamEngine.Utils.Strategy.Schema.BPSParseSchema
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession

/** 查看Parquet的脚本
 *
 * @author clock
 * @version 0.1
 * @since 2019/11/6 15:08
 * @note
 */
class ReadParquetScript extends FunSuite {
    val spark: SparkSession = BPSparkSession()

    val spark = BPSparkSession(null)
    val id = "08d72f26-f5cc-4618-911c-812a4a4e1cec"
    def byBatchForCsv(): Unit = {
        val jobId = "0090df5d-8858-46e3-a4fa-ce53e183e905"

//        val path = s"hdfs:///user/clock/jobs/$jobId/metadata"
        val path = s"hdfs:///user/clock/jobs/$jobId/contents"
//        val path = s"hdfs:///user/clock/jobs/$jobId/err"
//        val path = s"hdfs:///user/clock/jobs/$jobId/row_record"

        val reading = spark.read
                .format("com.databricks.spark.csv")
//                .option("header", "true")     //这里如果在csv第一行有属性的话，没有就是"false"
                .option("inferSchema", true.toString)   //这是自动推断属性列的数据类型。
                .load(path) //文件的路径

        reading.show(false)
        println(reading.count())
    }

    val testPath = "hdfs://jobs/01e951cb-34ed-4644-9a9e-fc3f570e0613/c65c732e-fec8-41ea-b891-ef737837a354/contents"
    val metadataPath = "hdfs://jobs/01e951cb-34ed-4644-9a9e-fc3f570e0613/c65c732e-fec8-41ea-b891-ef737837a354/metadata"
    def byBatch(): Unit = {
//        val loadSchema = BPSParseSchema.parseSchemaByMetadata(metadataPath)(spark)
//        val reading = spark.read.schema(loadSchema).parquet(testPath)
//        reading.show(false)
//        println(reading.count())
    }

    // FIX 测试未通过
    test("read parquet use stream") {
        val metadataPath = "/jobs/02c07385-39fa-496a-a9ac-029ed09aa79c/0db59660-056f-4d48-9ade-90b8ceaadc57/metadata"
        val filePath = "/jobs/02c07385-39fa-496a-a9ac-029ed09aa79c/0db59660-056f-4d48-9ade-90b8ceaadc57/contents"

        val loadSchema = BPSParseSchema.parseSchemaByMetadata(metadataPath)(spark)
        val reading = spark.readStream
                .schema(loadSchema)
                .option("startingOffsets", "earliest")
                .parquet(filePath)

        val wordCounts = reading.groupBy("2#年").count()

        val query = wordCounts.writeStream
                .outputMode("complete")
                .format("console")
                .start()
        query.awaitTermination()
    }
}
