package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxConvertSchemaJobContainer

import java.io.{BufferedWriter, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit
import java.util.{Collections, UUID}

import com.pharbers.StreamEngine.Jobs.SandBoxJob.Automation.BPSAutomationJob
import com.pharbers.StreamEngine.Jobs.SandBoxJob.BloodJob.BPSBloodJob
import com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxConvertSchemaJobContainer.Listener.ConvertSchemaListener
import com.pharbers.StreamEngine.Jobs.SandBoxJob.SchemaConverter
import com.pharbers.StreamEngine.Jobs.SandBoxJob.UploadEndJob.BPSUploadEndJob
import com.pharbers.StreamEngine.Utils.Component.Dynamic.JobMsg
import com.pharbers.StreamEngine.Utils.HDFS.BPSHDFSFile
import com.pharbers.StreamEngine.Utils.HDFS.BPSHDFSFile.configuration
import com.pharbers.StreamEngine.Utils.Schema.Spark.BPSMetaData2Map
import com.pharbers.StreamEngine.Utils.StreamJob.BPSJobContainer
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy
import com.pharbers.kafka.producer.PharbersKafkaProducer
import com.pharbers.kafka.schema.{BPJob, DataSet, OssTaskResult, UploadEnd}
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
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
	
	// TODO: 想个办法把这两个东西搞出去
	var totalRow: Long = 0
	var tid = ""
	
	override def open(): Unit = {
		
		notFoundShouldWait(s"${jobParam("parentMetaData")}/${jobParam("parentJobId")}")
		val metaData = spark.sparkContext.textFile(s"${jobParam("parentMetaData")}/${jobParam("parentJobId")}")
		val (schemaData, colNames, tabName, length, traceId) =
			writeMetaData(metaData, jobParam("metaDataSavePath") + jobParam("currentJobId"))
		totalRow = length
		tid = traceId
		
//		if (schemaData != "") {
//
//		}
		
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
				.select(
					from_json($"data", schema).as("data")
				).select("data.*")
		)
		
		// 暂时注释
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
		// TODO 将处理好的Schema发送邮件
		
		val automationResp = new OssTaskResult(jobParam("jobContainerId"), tid, 100.toLong, "")
		BPSAutomationJob("oss_task_response", automationResp).exec()
		
		// 临时放入
//		pushPyjob(
//			id,
//			jobParam("metaDataSavePath") + jobParam("currentJobId"),
//			jobParam("parquetSavePath") + jobParam("currentJobId"),
//			jobParam("currentJobId"),
//			(metaDataSetId :: sampleDataSetId :: Nil).mkString(",")
//		)
		
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
			
			
			val contentMap = BPSMetaData2Map.list2Map(metaDataDF.select("MetaData").collect().toList.map(_.getAs[String]("MetaData")))
			implicit val formats: DefaultFormats.type = DefaultFormats
			val schema  = write(contentMap("schema").asInstanceOf[List[Map[String, Any]]])
			
			val jobIdRow = metaDataDF.withColumn("MetaData",
				lit(s"""{"jobId":"${jobParam("currentJobId")}"}"""))
			val traceId = jobParam("parentJobId").substring(0, jobParam("parentJobId").length-1)
			val traceIdRow = jobIdRow.withColumn("MetaData",
				lit(s"""{"traceId":"$traceId"}"""))
			
			val metaDataDis = metaDataDF.union(jobIdRow).union(traceIdRow).distinct()
			
			// BPSHDFSFile.
			metaDataDis.collect().foreach(x => appendLine2HDFS(path, x.getAs[String]("MetaData")))
			val colNames =  contentMap("schema").asInstanceOf[List[Map[String, Any]]].map(_("key").toString)
			val tabName = contentMap.getOrElse("tag", Map.empty).
				asInstanceOf[Map[String, Any]].
				getOrElse("sheetName", "").toString
			(schema, colNames, tabName, contentMap("length").toString.toInt, traceId)
		} catch {
			case e: Exception =>
				// TODO: 处理不了发送重试
				logger.error(e.getMessage)
				("", Nil, "", 0, "")
		}
		
	}
	
	def appendLine2HDFS(path: String, line: String): Unit = {
		val fileSystem: FileSystem = FileSystem.get(configuration)
		val hdfsWritePath: Path = new Path(path)
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
		
		// 临时放入 自动化压测用
//	private def pushPyjob(runId: String,
//	                      metadataPath: String,
//	                      filesPath: String,
//	                      parentJobId: String,
//	                      dsIds: String): Unit = {
//		val resultPath = s"hdfs:///user/alex/jobs/$runId/${UUID.randomUUID().toString}/contents/"
//		import org.json4s._
//		import org.json4s.jackson.Serialization.write
//		implicit val formats: DefaultFormats.type = DefaultFormats
//		val traceId = ""
//		val `type` = "add"
//		val jobConfig = Map(
//			"jobId" -> parentJobId,
//			"parentsOId" -> dsIds,
//			"metadataPath" -> metadataPath,
//			"filesPath" -> filesPath,
//			"resultPath" -> resultPath
//		)
//		val job = JobMsg(
//			s"ossPyJob$parentJobId",
//			"job",
//			"com.pharbers.StreamEngine.Jobs.PyJob.PythonJobContainer.BPSPythonJobContainer",
//			List("$BPSparkSession"),
//			Nil,
//			Nil,
//			jobConfig,
//			"",
//			"temp job")
//		val jobMsg = write(job)
//		val topic = "stream_job_submit"
//		val pkp = new PharbersKafkaProducer[String, BPJob]
//		val bpJob = new BPJob(parentJobId, traceId, `type`, jobMsg)
//		val fu = pkp.produce(topic, parentJobId, bpJob)
//		logger.debug(fu.get(10, TimeUnit.SECONDS))
//	}
}
