package com.pharbers.StreamEngine.Jobs.SandBoxJob.ConvertMAX5

import java.io.{BufferedWriter, OutputStreamWriter}
import java.nio.charset.StandardCharsets

import com.pharbers.StreamEngine.Jobs.SandBoxJob.ConvertMAX5.Listener.BPSMAXConvertListener
import com.pharbers.StreamEngine.Jobs.SandBoxJob.SchemaConverter
import com.pharbers.StreamEngine.Utils.StreamJob.BPSJobContainer
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{from_json, lit, regexp_replace}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

object BPSConvertMAX5JobContainer {
	def apply(id: String,
	          metaPath: String,
	          samplePath: String,
	          fileName: String,
	          jobId: String,
	          tailJobIds: List[(String ,String)],
	          spark: SparkSession): BPSConvertMAX5JobContainer =
		new BPSConvertMAX5JobContainer(id, metaPath, samplePath, jobId, fileName, tailJobIds, spark)
}

class BPSConvertMAX5JobContainer(val id: String,
                                 metaPath: String,
                                 samplePath: String,
                                 fileName: String,
                                 jobId: String,
                                 tailJobIds: List[(String ,String)],
                                 val spark: SparkSession) extends BPSJobContainer{
	type T = BPSKfkJobStrategy
	val strategy: T = null
	import spark.implicits._
	
	var totalRow: Long = 0
	override def open(): Unit = {
		println("%%%%%%%%%%=> Start")
		println("^^^^^^^^^=> " + jobId)
		val metaData = SchemaConverter.column2legal("MetaData",spark.sparkContext
			.textFile(s"$metaPath/$jobId")
			.toDF("MetaData"))


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
				pushLineToHDFS(id, jobId, line)
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
				val query = is.writeStream
					.outputMode("append")
					.format("parquet")
//					.format("console")
					.option("checkpointLocation", s"/test/alex/$id/$jobId/checkpoint")
					.option("path", s"/test/alex/$id/$jobId/files")
					.start()
				outputStream = query :: outputStream
				
				val listener = BPSMAXConvertListener(id, jobId, tailJobIds, spark, this, query, totalRow, metaPath, samplePath)
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
	
	def pushLineToHDFS(runId: String, jobId: String, line: String): Unit = {
		val configuration: Configuration = new Configuration
		configuration.set("fs.defaultFS", "hdfs://192.168.100.137:9000")
		val fileSystem: FileSystem = FileSystem.get(configuration)
		//Create a path
		val hdfsWritePath: Path = new Path("/test/alex/" + runId + "/metadata/" + jobId + "")
		val fsDataOutputStream: FSDataOutputStream =
			if (fileSystem.exists(hdfsWritePath))
				fileSystem.append(hdfsWritePath)
			else
				fileSystem.create(hdfsWritePath)
		
		val bufferedWriter: BufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8))
		bufferedWriter.write(line)
		bufferedWriter.newLine()
		bufferedWriter.close()
	}
}
