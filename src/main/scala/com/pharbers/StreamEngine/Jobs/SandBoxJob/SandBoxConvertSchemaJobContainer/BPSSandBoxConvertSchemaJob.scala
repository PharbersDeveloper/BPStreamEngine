package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxConvertSchemaJobContainer

import java.util.{Collections, UUID}

import com.pharbers.StreamEngine.Jobs.SandBoxJob.BloodJob.BPSBloodJob
import com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxConvertSchemaJobContainer.Listener.ConvertSchemaListener
import com.pharbers.StreamEngine.Jobs.SandBoxJob.SchemaConverter
import com.pharbers.StreamEngine.Jobs.SandBoxJob.UploadEndJob.BPSUploadEndJob
import com.pharbers.StreamEngine.Utils.HDFS.BPSHDFSFile
import com.pharbers.StreamEngine.Utils.Schema.Spark.BPSMetaData2Map
import com.pharbers.StreamEngine.Utils.StreamJob.BPSJobContainer
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy
import com.pharbers.kafka.schema.{DataSet, UploadEnd}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization._

import collection.JavaConverters._

object BPSSandBoxConvertSchemaJob {
    def apply(id: String,
              jobParam: Map[String, String],
              spark: SparkSession,
              sampleDataSetId: String,
              metaDataSetId: String): BPSSandBoxConvertSchemaJob =
        new BPSSandBoxConvertSchemaJob(id, jobParam, spark, sampleDataSetId, metaDataSetId)
}

class BPSSandBoxConvertSchemaJob(val id: String,
                                 jobParam: Map[String, String],
                                 val spark: SparkSession,
                                 sampleDataSetId: String,
                                 metaDataSetId: String) extends BPSJobContainer {
	
	type T = BPSKfkJobStrategy
	val strategy: Null = null
	import spark.implicits._
	
	var totalRow: Long = 0
	
	override def open(): Unit = {
		
		notFoundShouldWait(s"${jobParam("parentMetaData")}/${jobParam("parentJobId")}")
		val metaData = spark.sparkContext.textFile(s"${jobParam("parentMetaData")}/${jobParam("parentJobId")}")
		val (schemaData, colNames, tabName, length, traceId) =
			writeMetaData(metaData, jobParam("metaDataSavePath") + jobParam("currentJobId"))
		totalRow = length
		
		val schema = SchemaConverter.str2SqlType(schemaData)
		
		notFoundShouldWait(jobParam("parentSampleData"))
		
		val reading = spark.readStream.schema(StructType(
				StructField("traceId", StringType) ::
				StructField("type", StringType) ::
				StructField("data", StringType) ::
				StructField("timestamp", TimestampType) ::
				StructField("jobId", StringType) :: Nil
			))
			.parquet(s"${jobParam("parentSampleData")}")
			.filter($"jobId" === jobParam("parentJobId") and $"type" === "SandBox")

		inputStream = Some(
			SchemaConverter.column2legal("data", reading).select(
				from_json($"data", schema).as("data")
			).select("data.*")
		)
		
		// MetaData DataSet
		BPSBloodJob(
			"data_set_job",
			new DataSet(
				Collections.emptyList(),
				metaDataSetId,
				jobParam("jobContainerId"),
				colNames.asJava,
				tabName,
				length,
				jobParam("metaDataSavePath") + jobParam("currentJobId"),
				"")).exec()
		
		// SampleData DataSet
		BPSBloodJob(
			"data_set_job",
			new DataSet(
				Collections.emptyList(),
				sampleDataSetId,
				jobParam("jobContainerId"),
				colNames.asJava,
				tabName,
				length,
				jobParam("parquetSavePath") + jobParam("currentJobId"),
				"")).exec()
		
		val uploadEnd = new UploadEnd(sampleDataSetId, traceId)
		BPSUploadEndJob("upload_end_job", uploadEnd).exec()
		
	}
	
	override def exec(): Unit = {
		inputStream match {
			case Some(is) =>
				val query = is.writeStream
    					.outputMode("append")
    					.format("parquet")
    					.option("checkpointLocation", jobParam("checkPointSavePath"))
    					.option("path", jobParam("parquetSavePath") + jobParam("currentJobId"))
					    .start()
				outputStream = query :: outputStream
				val listener = ConvertSchemaListener(id, jobParam("parentJobId"), spark, this, query, totalRow)
				listener.active(null)
				listeners = listener :: listeners
			case None =>
		}
	}
	
	override def close(): Unit = {
		outputStream.foreach(_.stop())
		listeners.foreach(_.deActive())
	}
	
	def notFoundShouldWait(path: String): Unit = {
		if (!BPSHDFSFile.checkPath(path)) {
			logger.debug(path + "文件不存在，等待 1s")
			Thread.sleep(1000)
			notFoundShouldWait(path)
		}
	}
	
	def writeMetaData(metaData: RDD[String], path: String): (String, List[CharSequence], String, Int, String) = {
		val contentMap = BPSMetaData2Map.list2Map(metaData.collect().toList.map(_.replaceAll("""\\"""", "")))
		implicit val formats: DefaultFormats.type = DefaultFormats
		val schema  = write(contentMap("schema").asInstanceOf[List[Map[String, Any]]])
		
		val metaDataDF = SchemaConverter.column2legal("MetaData", metaData.toDF("MetaData"))
		
		val jobIdRow = metaDataDF.withColumn("MetaData",
			lit(s"""{"jobId":"${jobParam("parentJobId")}"}"""))
		val traceId = jobParam("parentJobId").substring(0, jobParam("parentJobId").length-1)
		val traceIdRow = jobIdRow.withColumn("MetaData",
			lit(s"""{"traceId":"$traceId"}"""))
		
		val metaDataDis = metaDataDF.union(jobIdRow).union(traceIdRow).distinct()
		
		metaDataDis.collect().foreach { x =>
			val line = x.getAs[String]("MetaData")
			BPSHDFSFile.appendLine2HDFS(path, line)
		}
		val colNames =  contentMap("schema").asInstanceOf[List[Map[String, Any]]].map(_("key").toString)
		val tabName = contentMap.getOrElse("tag", Map.empty).asInstanceOf[Map[String, Any]].getOrElse("sheetName", "").toString
		(schema, colNames, tabName, contentMap("length").toString.toInt, traceId)
	}
}
