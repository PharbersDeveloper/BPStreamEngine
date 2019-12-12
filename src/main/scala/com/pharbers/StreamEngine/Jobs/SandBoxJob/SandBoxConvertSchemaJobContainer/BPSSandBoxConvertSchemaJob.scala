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
              dataSetId: String): BPSSandBoxConvertSchemaJob =
        new BPSSandBoxConvertSchemaJob(id, jobParam, spark, dataSetId)
}

class BPSSandBoxConvertSchemaJob(val id: String,
                                 jobParam: Map[String, String],
                                 val spark: SparkSession,
                                 dataSetId: String) extends BPSJobContainer {
	
	type T = BPSKfkJobStrategy
	val strategy: Null = null
	import spark.implicits._
	
	// TODO: 想个办法把这个东西搞出去
	var totalRow: Long = 0
//	var columnNames: List[CharSequence] = Nil
//	var sheetName: String = ""
//	var dataAssetId: String = ""
	
	override def open(): Unit = {
		
		notFoundShouldWait(s"${jobParam("parentMetaData")}/${jobParam("parentJobId")}")
		val metaData = spark.sparkContext.textFile(s"${jobParam("parentMetaData")}/${jobParam("parentJobId")}")
		val (schemaData, colNames, tabName, length, assetId) =
			writeMetaData(metaData, s"${jobParam("metaDataSavePath")}")
		totalRow = length
//		columnNames = colNames
//		sheetName = tabName
//		dataAssetId = assetId
		
		if (schemaData.isEmpty || schemaData == "") {
			logger.warn("Schema Is Null")
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
			
			
			if (assetId.isEmpty) {
				logger.info(s"AssetId Is Null ====> $assetId, Path ====> ${jobParam("parquetSavePath")}")
				logger.info(s"AssetId Is Null ====> $assetId, Path ====> ${jobParam("metaDataSavePath")}")
			}
			
			BPSBloodJob(
				"data_set_job",
				new DataSet(
					Collections.emptyList(),
					dataSetId,
					jobParam("jobContainerId"),
					colNames.asJava,
					tabName,
					length,
					s"${jobParam("parquetSavePath")}",
					"SampleData")).exec()
			
			val uploadEnd = new UploadEnd(dataSetId, assetId)
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
    					.option("path", s"${jobParam("parquetSavePath")}")
					    .start()
				
				logger.debug(s"Parquet Save Path Your Path =======> ${jobParam("parquetSavePath")}")
				
				outputStream = query :: outputStream
				val listener = ConvertSchemaListener(id, jobParam("parentJobId"), spark, this, query, totalRow)
				listener.active(null)
				listeners = listener :: listeners
			case None => logger.warn("Stream Is Null")
		}
	}
	
	override def close(): Unit = {
		// TODO 将处理好的Schema发送邮件
		
//		BPSBloodJob(
//			"data_set_job",
//			new DataSet(
//				Collections.emptyList(),
//				dataSetId,
//				jobParam("jobContainerId"),
//				columnNames.asJava,
//				sheetName,
//				totalRow.toInt,
//				s"${jobParam("parquetSavePath")}",
//				"SampleData")).exec()
//
//		val uploadEnd = new UploadEnd(dataSetId, dataAssetId)
//		BPSUploadEndJob("upload_end_job", uploadEnd).exec()
		
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
			// TODO: 既然啥都没做，为啥要这样写，直接COPY不就行了，后面再改吧
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
