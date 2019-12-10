package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxConvertSchemaJobContainer

import java.util.Collections

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
	
	// TODO: 想个办法把这个东西搞出去
	var totalRow: Long = 0
	
	override def open(): Unit = {
		
		notFoundShouldWait(s"${jobParam("parentMetaData")}/${jobParam("parentJobId")}")
		val metaData = spark.sparkContext.textFile(s"${jobParam("parentMetaData")}/${jobParam("parentJobId")}")
		val (schemaData, colNames, tabName, length, assetId) =
			writeMetaData(metaData, jobParam("metaDataSavePath") + jobParam("currentJobId"))
		totalRow = length
		
		if (schemaData.isEmpty || schemaData == "") {
			logger.warn("Schema Is Null，又是一个空的")
			this.close()
		} else {
			val schema = SchemaConverter.str2SqlType(schemaData)
			
			notFoundShouldWait(jobParam("parentSampleData"))
			
			val reading = spark.readStream.schema(StructType(
			    StructField("traceId", StringType) ::
				StructField("type", StringType) ::
				StructField("data", StringType) ::
				StructField("timestamp", TimestampType) ::
				StructField("jobId", StringType) :: Nil
			)).parquet(s"${jobParam("parentSampleData")}")
				.filter($"jobId" === jobParam("parentJobId") and $"type" === "SandBox")
			
			inputStream = Some(
				SchemaConverter.column2legal("data", reading)
					.select(from_json($"data", schema).as("data"))
					.select("data.*")
			)
			
			// MetaData DataSet
			BPSBloodJob(
				"data_set_job",
				new DataSet(
					Collections.emptyList(),
					metaDataSetId,
					jobParam("jobContainerId"),
					Collections.emptyList(),
					"",
					0,
					jobParam("metaDataSavePath") + jobParam("currentJobId"),
					"MetaData")).exec()
			
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
					"SampleData")).exec()
			
			if (assetId.isEmpty) {
				logger.info(s"Fuck AssetId Is Null ====> $assetId, Path ====> ${jobParam("parquetSavePath")}${jobParam("currentJobId")}")
			}
			
			val uploadEnd = new UploadEnd(sampleDataSetId, assetId)
			BPSUploadEndJob("upload_end_job", uploadEnd).exec()
		}
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
				
				logger.debug(s"Parquet Save Path Your Path =======> ${jobParam("parquetSavePath")}${jobParam("currentJobId")}")
				
				outputStream = query :: outputStream
				val listener = ConvertSchemaListener(id, jobParam("parentJobId"), spark, this, query, totalRow)
				listener.active(null)
				listeners = listener :: listeners
			case None => logger.warn("流是个空的")
		}
	}
	
	override def close(): Unit = {
		// TODO 将处理好的Schema发送邮件
		
//		val automationResp = new OssTaskResult(jobParam("jobContainerId"), "", 100.toLong, "")
//		BPSAutomationJob("oss_task_response", automationResp).exec()
		
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
		try {
			val metaDataDF = SchemaConverter.column2legal("MetaData", metaData.toDF("MetaData"))
			val contentMap = BPSMetaData2Map.
				list2Map(metaDataDF.select("MetaData").collect().toList.map(_.getAs[String]("MetaData")))
			implicit val formats: DefaultFormats.type = DefaultFormats
			val schema  = write(contentMap("schema").asInstanceOf[List[Map[String, Any]]])
			
			metaDataDF.collect().foreach(x => BPSHDFSFile.appendLine2HDFS(path, x.getAs[String]("MetaData")))
			val colNames =  contentMap("schema").asInstanceOf[List[Map[String, Any]]].map(_("key").toString)
			val tabName = contentMap.
				getOrElse("tag", Map.empty).
				asInstanceOf[Map[String, Any]].
				getOrElse("sheetName", "").toString
			
			val assetId = contentMap.
				getOrElse("tag", Map.empty).
				asInstanceOf[Map[String, Any]].
				getOrElse("assetId", "").toString
			
			(schema, colNames, tabName, contentMap("length").toString.toInt, assetId)
		} catch {
			case e: Exception =>
				// TODO: 处理不了发送重试
				logger.error(e.getMessage)
				("", Nil, "", 0, "")
		}
		
	}
}
