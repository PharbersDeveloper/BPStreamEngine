package com.pharbers.StreamEngine.Others.clock

import com.pharbers.StreamEngine.Utils.Schema.Spark.BPSParseSchema
import com.pharbers.StreamEngine.Utils.Session.Spark.BPSparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

import scala.util.parsing.json.JSON

/** 查看Parquet的脚本
 *
 * @author clock
 * @version 0.1
 * @since 2019/11/6 15:08
 * @note
 */
object ReadParquetScript extends App {
    val id = "03586-4810-48ba-bb9e-be6680"
    val matedataPath = "hdfs:///test/alex/0829b025-48ac-450c-843c-6d4ee91765ca/metadata/"
    val filesPath = "hdfs:///test/alex/0829b025-48ac-450c-843c-6d4ee91765ca/files/"

    val spark = BPSparkSession()

    def byBatchForCsv(): Unit = {
//        val path = "hdfs:///test/qi3/abc001/metadata"
        val path = "hdfs:///test/qi3/abc001/file"
//        val path = "hdfs:///test/qi3/abc001/err"
        val reading = spark.read
                .format("com.databricks.spark.csv")
                .option("header", "true") //这里如果在csv第一行有属性的话，没有就是"false"
                .option("inferSchema", true.toString)//这是自动推断属性列的数据类型。
                .load(path) //文件的路径
        reading.show(false)
        println(reading.count())
    }
    byBatchForCsv()

    def byBatch(): Unit = {
        val loadSchema = BPSParseSchema.parseSchemaByMetadata(matedataPath + id)(spark)
        val reading = spark.read.schema(loadSchema).parquet(filesPath)
        reading.show(false)
    }
//    byBatch()

    def byStream(): Unit = {
        val loadSchema = BPSParseSchema.parseSchemaByMetadata(matedataPath + id)(spark)
        val reading = spark.readStream
                .schema(loadSchema)
                .option("startingOffsets", "earliest")
                .parquet(filesPath + id)

        val wordCounts = reading.groupBy("Year").count()

        val query = wordCounts.writeStream
                .outputMode("complete")
                .format("console")
                .start()
        query.awaitTermination()
    }
}
