package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxConvertSchemaJobContainer

import java.util.{Collections, UUID}
import java.util.concurrent.TimeUnit

import com.pharbers.StreamEngine.Jobs.SandBoxJob.BloodJob.BPSBloodJob
import com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxConvertSchemaJobContainer.Listener.ConvertSchemaListener
import com.pharbers.StreamEngine.Jobs.SandBoxJob.UploadEndJob.BPSUploadEndJob
import com.pharbers.StreamEngine.Utils.Component.Dynamic.JobMsg
import com.pharbers.StreamEngine.Utils.HDFS.BPSHDFSFile
import com.pharbers.StreamEngine.Utils.Schema.Spark.{BPSMetaData2Map, SchemaConverter}
import com.pharbers.StreamEngine.Utils.StreamJob.BPSJobContainer
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy
import com.pharbers.kafka.producer.PharbersKafkaProducer
import com.pharbers.kafka.schema.{BPJob, DataSet, UploadEnd}
import org.apache.avro.specific.SpecificRecord
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
		
		if (schemaData.isEmpty || assetId.isEmpty) {
			// TODO: metadata中缺少schema 和 asset标识，走错误流程
			logger.error("Schema Is Null")
			logger.error(s"AssetId Is Null ====> $assetId, Path ====> ${jobParam("parquetSavePath")}")
			logger.error(s"AssetId Is Null ====> $assetId, Path ====> ${jobParam("metaDataSavePath")}")
			this.close()
		} else {
			val schema = SchemaConverter.str2SqlType(schemaData)
			
			notFoundShouldWait(jobParam("parentSampleData"))
			logger.info(s"DCS Path Info ${jobParam("parentSampleData")}")
			val reading = spark.readStream.schema(StructType(
			    StructField("traceId", StringType) ::
				StructField("type", StringType) ::
				StructField("data", StringType) ::
				StructField("timestamp", TimestampType) ::
				StructField("jobId", StringType) :: Nil
			)).parquet(s"${jobParam("parentSampleData")}")
				.filter($"jobId" === jobParam("parentJobId") and $"type" === "SandBox")
			
			inputStream = Some(
				SchemaConverter.column2legalWithDF("data", reading)
					.select(from_json($"data", schema).as("data"))
					.select("data.*")
			)
//			// TODO: 这部分拿到任务结束在创建否则中间崩溃又要重新创建一次
			BPSBloodJob(
				"data_set_job_k8s_test",
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
			BPSUploadEndJob("upload_end_job_k8s_test", uploadEnd).exec()

			pushPyjob(
				id,
				s"${jobParam("metaDataSavePath")}",
				s"${jobParam("parquetSavePath")}",
				UUID.randomUUID().toString,
				(dataSetId :: Nil).mkString(",")
			)
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
			val primitive = BPSMetaData2Map.list2Map(metaData.collect().toList)
			val convertContent = primitive ++ SchemaConverter.column2legalWithMetaDataSchema(primitive)
			
			implicit val formats: DefaultFormats.type = DefaultFormats
			val schema  = write(convertContent("schema").asInstanceOf[List[Map[String, Any]]])
			val colNames =  convertContent("schema").asInstanceOf[List[Map[String, Any]]].map(_("key").toString)
			val tabName = convertContent.
				getOrElse("tag", Map.empty).
				asInstanceOf[Map[String, Any]].
				getOrElse("sheetName", "").toString

			val assetId = convertContent.
				getOrElse("tag", Map.empty).
				asInstanceOf[Map[String, Any]].
				getOrElse("assetId", "").toString
			// TODO: 这块儿还要改进
			convertContent.foreach { x =>
				if(x._1 == "length") {
					BPSHDFSFile.appendLine2HDFS(path, s"""{"length": ${x._2}}""")
				} else {
					BPSHDFSFile.appendLine2HDFS(path, write(x._2))
				}
			}
			(schema, colNames, tabName, convertContent("length").toString.toInt, assetId)
			
			
//			val metaDataDF = SchemaConverter.column2legalWithDF("MetaData", metaData.toDF("MetaData"))
//			val contentMap = BPSMetaData2Map.
//				list2Map(metaDataDF.select("MetaData").collect().toList.map(_.getAs[String]("MetaData")))
//			implicit val formats: DefaultFormats.type = DefaultFormats
//			val schema  = write(contentMap("schema").asInstanceOf[List[Map[String, Any]]])
//			metaDataDF.collect().foreach(x => BPSHDFSFile.appendLine2HDFS(path, x.getAs[String]("MetaData")))
//
//			val colNames =  contentMap("schema").asInstanceOf[List[Map[String, Any]]].map(_("key").toString)
//			val tabName = contentMap.
//				getOrElse("tag", Map.empty).
//				asInstanceOf[Map[String, Any]].
//				getOrElse("sheetName", "").toString
//
//			val assetId = contentMap.
//				getOrElse("tag", Map.empty).
//				asInstanceOf[Map[String, Any]].
//				getOrElse("assetId", "").toString
//
//			(schema, colNames, tabName, contentMap("length").toString.toInt, assetId)
		} catch {
			case e: Exception =>
				// TODO: 处理不了发送重试
				logger.error(e.getMessage)
				("", Nil, "", 0, "")
		}
		
	}
	
	// TODO：因还未曾与老齐对接口，暂时放到这里
	private def pushPyjob(runId: String,
	                      metadataPath: String,
	                      filesPath: String,
	                      parentJobId: String,
	                      dsIds: String): Unit = {
		val resultPath = s"/jobs/$runId/"
		
		import org.json4s._
		import org.json4s.jackson.Serialization.write
		implicit val formats: DefaultFormats.type = DefaultFormats
		val traceId = ""
		val `type` = "add"
		val jobConfig = Map(
			"jobId" -> parentJobId,
			"parentsOId" -> dsIds,
			"metadataPath" -> metadataPath,
			"filesPath" -> filesPath,
			"resultPath" -> resultPath
		)
		val job = JobMsg(
			s"ossPyJob$parentJobId",
			"job",
			"com.pharbers.StreamEngine.Jobs.PyJob.PythonJobContainer.BPSPythonJobContainer",
			List("$BPSparkSession"),
			Nil,
			Nil,
			jobConfig,
			"",
			"temp job")
		val jobMsg = write(job)
		val topic = "stream_job_submit_k8s_test"
		val bpJob = new BPJob(parentJobId, traceId, `type`, jobMsg)
		val producerInstance = new PharbersKafkaProducer[String, SpecificRecord]
		val fu = producerInstance.produce(topic, parentJobId, bpJob)
		logger.debug(fu.get(10, TimeUnit.SECONDS))
		producerInstance.producer.close()
	}
}
