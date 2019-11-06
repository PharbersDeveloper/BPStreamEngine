package com.pharbers.StreamEngine.Jobs.SandBoxJob.ConvertMAX5

import com.pharbers.StreamEngine.Jobs.OssPartitionJob.OssPartitionMeta.BPSOssPartitionMeta
import com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxConvertSchemaJob.Listener.BPSConvertSchemaJob
import com.pharbers.StreamEngine.Jobs.SandBoxJob.SchemaConverter
import com.pharbers.StreamEngine.Utils.StreamJob.BPSJobContainer
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{from_json, lit, regexp_replace}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

object BPSConvertMAX5JobContainer {
	def apply(id: String,
	          metaPath: String,
	          samplePath: String,
	          fileName: String,
	          jobId: String,
	          spark: SparkSession): BPSConvertMAX5JobContainer =
		new BPSConvertMAX5JobContainer(id, metaPath, samplePath, jobId, fileName, spark)
}

class BPSConvertMAX5JobContainer(val id: String,
                                 metaPath: String,
                                 samplePath: String,
                                 fileName: String,
                                 jobId: String,
                                 val spark: SparkSession) extends BPSJobContainer{
	type T = BPSKfkJobStrategy
	val strategy: T = null
	import spark.implicits._
	
	var totalRow: Long = 0
	
	override def open(): Unit = {
		val metaData = SchemaConverter.column2legal("MetaData",spark.sparkContext
			.textFile(s"$metaPath/$jobId")
			.toDF("MetaData"))
			
//			.withColumn("MetaData", regexp_replace($"MetaData" , """\\"""", ""))
//			.withColumn("MetaData", regexp_replace($"MetaData" , " ", "_"))
		
		if (metaData.count() > 1) {
			val jobIdRow = metaData
				.withColumn("MetaData", lit(s"""{"jobId":"$jobId"}"""))
			val traceIdRow = jobIdRow
				.withColumn("MetaData", lit(s"""{"traceId":"${jobId.substring(0, jobId.length-1)}"}"""))
			val fileNameRow = traceIdRow
				.withColumn("MetaData", lit(s"""{"fileName":"${fileName}"}"""))
			
			val metaDataStream = metaData.union(jobIdRow).union(traceIdRow).union(fileNameRow).distinct()
			
			val repMetaDataStream = metaData.head()
				.getAs[String]("MetaData")
			
			metaDataStream.collect().foreach{x =>
				val line = x.getAs[String]("MetaData")
				if (line.contains("""{"length":""")) {
					totalRow = line.substring(line.indexOf(":") + 1)
						.replaceAll("_", "").replace("}", "").toLong
				}
//				BPSOssPartitionMeta.pushLineToHDFS(id, jobId, line)
			}
			
			val schema = SchemaConverter.str2SqlType(repMetaDataStream)
			inputStream = Some(
				spark.readStream
					.schema(StructType(
						StructField("traceId", StringType) ::
						StructField("type", StringType) ::
						StructField("data", StringType) ::
						StructField("timestamp", TimestampType) :: Nil
					))
					.parquet(s"$samplePath$jobId")
					.filter($"type" === "SandBox")
					.withColumn("data", regexp_replace($"data", """\\"""", ""))
					.withColumn("data", regexp_replace($"data", " ", "_"))
					.withColumn("data", regexp_replace($"data", "\\(", ""))
					.withColumn("data", regexp_replace($"data", "\\)", ""))
					.withColumn("data", regexp_replace($"data", "=", ""))
					.withColumn("data", regexp_replace($"data", "\\\\n|\\\\\t", ""))
					.select(
						from_json($"data", schema).as("data")
					).select("data.*")
			)
		}
	}
	
	override def exec(): Unit = {
		inputStream match {
			case Some(is) =>
				println(s"=====>>>$jobId")
				val query = is.writeStream
					.outputMode("append")
//					.format("parquet")
					.format("console")
//					.option("checkpointLocation", "/test/alex/" + id + "/checkpoint")
//					.option("path", "/test/alex/" + id + "/" + jobId + "/files")
//					.option("checkpointLocation", s"/test/alex/$id/$jobId/checkpoint")
//					.option("path", s"/test/alex/$id/$jobId/files")
					.start()
				outputStream = query :: outputStream
				
				val listener = BPSConvertSchemaJob(id, jobId, spark, this, query, totalRow)
				listener.active(null)
				listeners = listener :: listeners
			
			case None =>
		}
	}
	
	override def close(): Unit = {
		outputStream.foreach(_.stop())
		listeners.foreach(_.deActive())
		inputStream match {
			case Some(_) =>
			case None =>
		}
	}
}
