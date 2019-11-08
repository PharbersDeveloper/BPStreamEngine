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
    val id = "57fe0-2bda-4880-8301-dc55a0"
    val matedataPath = "hdfs:///test/alex/07b8411a-5064-4271-bfd3-73079f2b42b2/metadata/"
    val filesPath = "hdfs:///test/alex/07b8411a-5064-4271-bfd3-73079f2b42b2/files/"

    val spark = BPSparkSession()

    def byBatchForCsv(): Unit = {
        val reading = spark.read
                .format("com.databricks.spark.csv")
                .option("header", "true") //这里如果在csv第一行有属性的话，没有就是"false"
                .option("inferSchema", true.toString)//这是自动推断属性列的数据类型。
                .load("hdfs:///test/qi/57fe0-2bda-4880-8301-dc55a0/file") //文件的路径
        reading.show(false)
    }
    byBatchForCsv()

    def byBatch(): Unit = {
        val reading = spark.read.parquet(filesPath)
        reading.show(false)
    }

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
