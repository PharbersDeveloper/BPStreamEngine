package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxConvertSchemaJobContainer

import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets

import com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxConvertSchemaJobContainer.Listener.ConvertSchemaListener
import com.pharbers.StreamEngine.Jobs.SandBoxJob.SchemaConverter
import com.pharbers.StreamEngine.Utils.HDFS.BPSHDFSFile
import com.pharbers.StreamEngine.Utils.StreamJob.BPSJobContainer
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.json4s.jackson.Serialization.write

object BPSSandBoxConvertSchemaJob {
    def apply(id: String,
              metaPath: String,
              samplePath: String,
              jobId: String,
              spark: SparkSession): BPSSandBoxConvertSchemaJob =
        new BPSSandBoxConvertSchemaJob(id, metaPath, samplePath, jobId, spark)
}

class BPSSandBoxConvertSchemaJob(val id: String,
                                 metaPath: String,
                                 samplePath: String,
                                 jobId: String,
                                 val spark: SparkSession) extends BPSJobContainer {
	
	type T = BPSKfkJobStrategy
	val strategy: Null = null
	import spark.implicits._
	
	var totalRow: Long = 0
	
	override def open(): Unit = {
		// TODO 要形成过滤规则参数化
		val metaData = SchemaConverter.column2legal("MetaData",spark.sparkContext
			.textFile(s"$metaPath/$jobId")
			.toDF("MetaData"))
		
		if (metaData.count() > 1) {
			val jobIdRow = metaData
				.withColumn("MetaData", lit(s"""{"jobId":"$jobId"}"""))
			
			val traceIdRow = jobIdRow
				.withColumn("MetaData", lit(s"""{"traceId":"${jobId.substring(0, jobId.length-1)}"}"""))
			
			val metaDataStream = metaData.union(jobIdRow).union(traceIdRow).distinct()
			
			val repMetaDataStream = metaData.head()
				.getAs[String]("MetaData")
			
			val path = s"/test/alex/$id/metadata/$jobId"
			
			metaDataStream.collect().foreach{x =>
				val line = x.getAs[String]("MetaData")
				if (line.contains("""{"length":""")) {
					totalRow = line.substring(line.indexOf(":") + 1)
						.replaceAll("_", "").replace("}", "").toLong
				}
				BPSHDFSFile.appendLine2HDFS(path, line)
			}
			
			val json = s"""{"jobId":"$jobId","path":"$path"}"""
			
			post(json, "application/json")
			
			val schema = SchemaConverter.str2SqlType(repMetaDataStream)
			BPSHDFSFile.checkPath(s"$samplePath")
			val reading = spark.readStream.schema(StructType(
					StructField("traceId", StringType) ::
						StructField("type", StringType) ::
						StructField("data", StringType) ::
						StructField("timestamp", TimestampType) ::
						StructField("jobId", StringType) :: Nil
				))
				.parquet(s"$samplePath")
				.filter($"jobId" === jobId and $"type" === "SandBox")
			
			inputStream = Some(
				SchemaConverter.column2legal("data", reading).select(
					from_json($"data", schema).as("data")
				).select("data.*")
			)
		}
	}
	
	override def exec(): Unit = {
		inputStream match {
			case Some(is) =>
				val query = is.writeStream
    					.outputMode("append")
    					.format("parquet")
    					.option("checkpointLocation", s"/test/alex/$id/files/$jobId/checkpoint")
    					.option("path", s"/test/alex/$id/files/$jobId").start()

				outputStream = query :: outputStream
				
				val listener = ConvertSchemaListener(id, jobId, spark, this, query, totalRow)
				listener.active(null)
				listeners = listener :: listeners
				
			case None =>
		}
	}
	
	override def close(): Unit = {
		outputStream.foreach(_.stop())
		listeners.foreach(_.deActive())
	}
	
	def post(body: String, contentType: String): Unit = {
		val conn = new URL("http://192.168.100.116:8080/updateInfoWithJobId").openConnection.asInstanceOf[HttpURLConnection]
		val postDataBytes = body.getBytes(StandardCharsets.UTF_8)
		conn.setRequestMethod("POST")
		conn.setRequestProperty("Content-Type", contentType)
		conn.setRequestProperty("Content-Length", String.valueOf(postDataBytes.length))
		conn.setConnectTimeout(60000)
		conn.setReadTimeout(60000)
		conn.setDoOutput(true)
		conn.getOutputStream.write(postDataBytes)
		conn.getResponseCode
	}
}
