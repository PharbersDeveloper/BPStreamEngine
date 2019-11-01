package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxConvertSchemaJob

import java.io.{BufferedWriter, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.TimeUnit

import com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxConvertSchemaJob.Listener.BPSConvertSchemaJob
import com.pharbers.StreamEngine.Utils.StreamJob.BPSJobContainer
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy
import com.pharbers.kafka.producer.PharbersKafkaProducer
import com.pharbers.kafka.schema.FileMetaData
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{BinaryType, BooleanType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read

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
		// TODO 要形成过滤规则参数化
		val metaData = spark.sparkContext
			.textFile(s"$metaPath/$jobId")
			.toDF("MetaData")
			.withColumn("MetaData", regexp_replace($"MetaData" , """\\"""", ""))
			.withColumn("MetaData", regexp_replace($"MetaData" , " ", "_"))
		
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
			pushLineToHDFS(id, jobId, line)
		}
		
		val schema = event2SqlType(repMetaDataStream)
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
				.withColumn("data", regexp_replace($"data" , """\\"""", ""))
				.withColumn("data", regexp_replace($"data" , " ", "_"))
				.withColumn("jobId", lit(jobId))
				.select(
					from_json($"data", schema).as("data")
				).select("data.*")
		)
	}
	
	override def exec(): Unit = {
		inputStream match {
			case Some(is) =>
				val query = is.writeStream
//    				.partitionBy("jobId")
					.outputMode("append")
					.format("parquet")
//					.format("console")
					.option("checkpointLocation", "/test/alex/" + id + "/checkpoint")
					.option("path", "/test/alex/" + id + "/" + jobId + "/files")
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
	
	def event2SqlType(data: String): org.apache.spark.sql.types.DataType = {
		implicit val formats = DefaultFormats
		val lst = read[List[BPSchemaParseElement]](data)
		StructType(
			lst.map(x => StructField(x.key, x.`type` match {
				case "String" => StringType
				case "Int" => IntegerType
				case "Boolean" => BooleanType
				case "Byte" => BinaryType
				case "Double" => DoubleType
				case "Float" => FloatType
				case "Long" => LongType
				case "Fixed" => BinaryType
				case "Enum" => StringType
			}))
		)
		
	}
	
}

case class BPSchemaParseElement(key: String, `type`: String)
