package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxConvertSchemaJobContainer

import java.util.{Collections, UUID}
import java.util.concurrent.TimeUnit

import com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxConvertSchemaJobContainer.Listener.ConvertSchemaListener
import com.pharbers.StreamEngine.Jobs.SandBoxJob.SchemaConverter
import com.pharbers.StreamEngine.Utils.HDFS.BPSHDFSFile
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
		
		submitDataSet(Nil, colNames, tabName, length, "")
		
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
		updateJob("pending", "", "")
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
				updateJob("exec", "", "")
			case None => updateJob("error", "inputStream is None", "")
		}
	}
	
	override def close(): Unit = {
		outputStream.foreach(_.stop())
		listeners.foreach(_.deActive())
		updateJob("success", "", "")
	}
	
	def notFoundShouldWait(path: String): Unit = {
		if (!BPSHDFSFile.checkPath(path)) {
			logger.debug(path + "文件不存在，等待 1s")
			Thread.sleep(1000)
			notFoundShouldWait(path)
		}
	}
	
	def writeMetaData(metaData: RDD[String], path: String): (String, List[String], String, Int) = {
		
		val contentMap = metaDataContent2Map(metaData.collect().toList.map(_.replaceAll("""\\"""", "")))
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
	
	def metaDataContent2Map(lst: List[String]): Map[String, Any] = {
		implicit val formats: DefaultFormats.type = DefaultFormats
		var jsonMap: Map[String, Any] = Map()
		lst.foreach{ line =>
			try {
				val schema = read[List[Map[String, Any]]](line)
				jsonMap = jsonMap ++ Map("schema" -> schema)
			} catch {
				case _: Throwable =>
					val obj = read[Map[String, Any]](line)
					obj match {
						case o if o.contains("label") => jsonMap = jsonMap ++ Map("tag" -> o)
						case o if o.contains("length") => jsonMap = jsonMap ++ Map("length" -> o("length"))
						case _ =>
					}
			}
		}
		jsonMap
	}
	
	def submitDataSet(parentNode: List[CharSequence], colName: List[CharSequence], tabName: String, length: Int, des: String): Unit = {
		val topic = "data_set_job"
		val dfs = new DataSet(parentNode.asJava, JOBID, colName.asJava, tabName, length, PARQUETSAVEPATH, des)
		pollKafka(topic,  dfs)
	}
	
	def updateJob(status: String, error: String, des: String): Unit = {
		val topic = "job_status"
		val job = new Job(JOBID, status,error,des)
		pollKafka(topic, job)
	}
	
	def pollKafka(topic: String, msg: SpecificRecord): Unit ={
		//TODO: 参数化
		val pkp = new PharbersKafkaProducer[String, SpecificRecord]
		val fu = pkp.produce(topic, JOBID, msg)
		logger.info(fu.get(10, TimeUnit.SECONDS))
	}
}
