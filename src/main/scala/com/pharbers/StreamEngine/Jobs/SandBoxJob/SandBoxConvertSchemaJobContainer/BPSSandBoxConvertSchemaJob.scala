package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxConvertSchemaJobContainer

import java.util.{Collections, UUID}
import java.util.concurrent.TimeUnit

import com.pharbers.StreamEngine.Jobs.SandBoxJob.BloodJob.BPSBloodJob
import com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxConvertSchemaJobContainer.Listener.ConvertSchemaListener
import com.pharbers.StreamEngine.Jobs.SandBoxJob.SchemaConverter
import com.pharbers.StreamEngine.Utils.Convert.BPSMetaData2Map
import com.pharbers.StreamEngine.Utils.HDFS.BPSHDFSFile
import com.pharbers.StreamEngine.Utils.Status.BPSJobStatus
import com.pharbers.StreamEngine.Utils.StreamJob.BPSJobContainer
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy
import com.pharbers.kafka.producer.PharbersKafkaProducer
import com.pharbers.kafka.schema.{DataSet, FileMetaData, Job}
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
	
	final val JOBID: String = UUID.randomUUID().toString
	final val ID: String = UUID.randomUUID().toString
	final val METADATASAVEPATH: String = s"/test/alex2/$ID/metadata/$JOBID"
	final val CHECKPOINTSAVEPATH: String = s"/test/alex2/$ID/files/$JOBID/checkpoint"
	final val PARQUETSAVEPATH: String =  s"/test/alex2/$ID/files/$JOBID"
	
	override def open(): Unit = {
		
		notFoundShouldWait(s"$metaPath/$jobId")
		val metaData = spark.sparkContext.textFile(s"$metaPath/$jobId")
		val (schemaData, colNames, tabName, length) = writeMetaData(metaData, METADATASAVEPATH)
		totalRow = length
		
		val dfs = new DataSet(Collections.emptyList(), JOBID, colNames.asJava, tabName, length, PARQUETSAVEPATH, jobId)
		BPSBloodJob(JOBID, "data_set_job", dfs).exec()
		
		val schema = SchemaConverter.str2SqlType(schemaData)
		
		notFoundShouldWait(samplePath)
		
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
		
		val pendingJob = new Job(JOBID, BPSJobStatus.Pending.toString, "", "")
		BPSBloodJob(JOBID, "job_status", pendingJob).exec()
	}
	
	override def exec(): Unit = {
		inputStream match {
			case Some(is) =>
				val query = is.writeStream
    					.outputMode("append")
    					.format("parquet")
    					.option("checkpointLocation", CHECKPOINTSAVEPATH)
    					.option("path", PARQUETSAVEPATH)
					    .start()
				outputStream = query :: outputStream
				val listener = ConvertSchemaListener(id, jobId, spark, this, query, totalRow)
				listener.active(null)
				listeners = listener :: listeners
				
				val execJob = new Job(JOBID, BPSJobStatus.Exec.toString, "", "")
				BPSBloodJob(JOBID, "job_status", execJob).exec()
			case None =>
				val errorJob = new Job(JOBID, BPSJobStatus.Fail.toString, "inputStream is None", "")
				BPSBloodJob(JOBID, "job_status", errorJob).exec()
		}
	}
	
	override def close(): Unit = {
		val successJob = new Job(JOBID, BPSJobStatus.Success.toString, "", "")
		BPSBloodJob(JOBID, "job_status", successJob).exec()
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
	
	def writeMetaData(metaData: RDD[String], path: String): (String, List[CharSequence], String, Int) = {
		val contentMap = BPSMetaData2Map.list2Map(metaData.collect().toList.map(_.replaceAll("""\\"""", "")))
		implicit val formats: DefaultFormats.type = DefaultFormats
		val schema  = write(contentMap("schema").asInstanceOf[List[Map[String, Any]]])
		
		val metaDataDF = SchemaConverter.column2legal("MetaData", metaData.toDF("MetaData"))
		
		val jobIdRow = metaDataDF.withColumn("MetaData", lit(s"""{"jobId":"$jobId"}"""))
		val traceIdRow = jobIdRow.withColumn("MetaData", lit(s"""{"traceId":"${jobId.substring(0, jobId.length-1)}"}"""))
		
		val metaDataDis = metaDataDF.union(jobIdRow).union(traceIdRow).distinct()
		
		metaDataDis.collect().foreach { x =>
			val line = x.getAs[String]("MetaData")
			BPSHDFSFile.appendLine2HDFS(path, line)
		}
		val colNames =  contentMap("schema").asInstanceOf[List[Map[String, Any]]].map(_("key").toString)
		val tabName = contentMap("tag").asInstanceOf[Map[String, Any]]("sheetName").toString
		(schema, colNames, tabName, contentMap("length").toString.toInt)
	}
}