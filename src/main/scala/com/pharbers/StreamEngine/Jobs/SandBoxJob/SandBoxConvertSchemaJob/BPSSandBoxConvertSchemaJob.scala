package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxConvertSchemaJob

import java.io.{BufferedWriter, OutputStreamWriter}
import java.nio.charset.StandardCharsets

import com.pharbers.StreamEngine.Jobs.OssPartitionJob.OssPartitionMeta.BPSOssPartitionMeta
import com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxConvertSchemaJob.Listener.BPSConvertSchemaJob
import com.pharbers.StreamEngine.Jobs.SandBoxJob.SchemaConverter
import com.pharbers.StreamEngine.Utils.StreamJob.BPSJobContainer
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

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
	val strategy = null
	import spark.implicits._
	
	var totalRow: Long = 0
	
	override def open(): Unit = {
		// 延迟2分钟，因HDFS存储大文件需要再机器中传输
//		Thread.sleep(1000 * 60 * 2)
		// TODO 要形成过滤规则参数化
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
			
			val metaDataStream = metaData.union(jobIdRow).union(traceIdRow).distinct()
			
			val repMetaDataStream = metaData.head()
				.getAs[String]("MetaData")
			
			metaDataStream.collect().foreach{x =>
				val line = x.getAs[String]("MetaData")
				if (line.contains("""{"length":""")) {
					totalRow = line.substring(line.indexOf(":") + 1)
						.replaceAll("_", "").replace("}", "").toLong
				}
//				BPSOssPartitionMeta.pushLineToHDFS(id, jobId, line)
				pushLineToHDFS(id, jobId, line)
			}
			
			val schema = SchemaConverter.str2SqlType(repMetaDataStream)
			checkPath(s"$samplePath")
			inputStream = Some(
				spark.readStream
					.schema(StructType(
						StructField("traceId", StringType) ::
							StructField("type", StringType) ::
							StructField("data", StringType) ::
							StructField("timestamp", TimestampType) ::
							StructField("jobId", StringType) :: Nil
					))
					.parquet(s"$samplePath")
					.filter($"jobId" === jobId and $"type" === "SandBox")
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
						.option("checkpointLocation", s"/test/alex/$id/files/$jobId/checkpoint")
						.option("path", s"/test/alex/$id/files/$jobId")
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
	
    def checkPath(path: String): Unit = {
        val configuration: Configuration = new Configuration
        configuration.set("fs.defaultFS", "hdfs://192.168.100.137:9000")
        val fileSystem: FileSystem = FileSystem.get(configuration)
        //Create a path
        if (!fileSystem.exists(new Path(path)))
            fileSystem.mkdirs(new Path(path))
    }
}

case class BPSchemaParseElement(key: String, `type`: String)
