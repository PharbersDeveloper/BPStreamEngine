package com.pharbers.StreamEngine.Jobs.SandBoxJob

import java.util.Collections

import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPJobLocalListener
import com.pharbers.StreamEngine.Utils.Job.{BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Utils.Strategy.Blood.BPSSetBloodStrategy
import com.pharbers.StreamEngine.Utils.Strategy.JobStrategy.BPSCommonJobStrategy
import com.pharbers.StreamEngine.Utils.Strategy.Queue.BPSSandBoxQueueStrategy
import com.pharbers.StreamEngine.Utils.Strategy.Schema.{BPSMetaData2Map, SchemaConverter}
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.msgMode.SparkQueryEvent
import com.pharbers.StreamEngine.Utils.Strategy.hdfs.BPSHDFSFile
import org.apache.kafka.common.config.ConfigDef
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.pharbers.kafka.schema.{DataSet, UploadEnd}
import org.apache.spark.sql
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write
import org.mongodb.scala.bson.ObjectId

import collection.JavaConverters._

case class BPSSandBoxConvertSchemaJob(container: BPSJobContainer,
                                      componentProperty: Component2.BPComponentConfig) extends BPStreamJob {
	
	type T = BPSCommonJobStrategy
	override val strategy: BPSCommonJobStrategy = BPSCommonJobStrategy(componentProperty.config, configDef)
	val bloodStrategy: BPSSetBloodStrategy = new BPSSetBloodStrategy(componentProperty.config)
	val queueStrategy: BPSSandBoxQueueStrategy = BPSSandBoxQueueStrategy(componentProperty.config)
	override val id: String = componentProperty.id // 本身Job的id
	val jobId: String = strategy.getJobId // componentProperty config中的job Id
	val runnerId: String = BPSConcertEntry.runner_id // Runner Id
	val traceId: String = componentProperty.args.head
	val msgType: String = componentProperty.args.last
	val spark: SparkSession = strategy.getSpark
	val sc: SchemaConverter = strategy.getSchemaConverter
	val hdfs: BPSHDFSFile = strategy.getHdfsFile
	var totalNum: Long = 0
//	val checkpointPath = s"/jobs/$runnerId/$id/checkpoint"
//	val parquetPath = s"/jobs/$runnerId/$id/contents"
//	val metaDataPath = s"/jobs/$runnerId/$id/metadata"
	val mongoId: String = new ObjectId().toString
	var metaData: Option[MetaData] = None
	implicit val formats: DefaultFormats.type = DefaultFormats
	
	import spark.implicits._
	
	override def open(): Unit = {
		inputStream = setInputStream(container.inputStream)
	}
	
	override def exec(): Unit = inputStream match {
		case Some(is) => {
			val query = startProcessParquet(is)
			outputStream = outputStream :+ query
			
			val rowNumListener =
				BPJobLocalListener[SparkQueryEvent](null, List(s"spark-${query.id.toString}-progress"))(x => {
					val cumulative = query.recentProgress.map(_.numInputRows).sum
					logger.info(s"cumulative num $cumulative")
					logger.info(s"progress status  =======>>> ${x.date.status}")
					logger.info(s"progress msg     =======>>> ${x.date.msg}")
					if (cumulative >= totalNum) {
						pushBloodMsg()
						this.close()
					}
				})
			rowNumListener.active(null)
			listeners = listeners :+ rowNumListener
		}
		case None => ???
	}
	
	override def close(): Unit = {
		logger.info("Job =====> Closed")
		super.close()
		queueStrategy.popExecJobNum()
		container.finishJobWithId(id)
	}
	
	def startProcessParquet(df: DataFrame): StreamingQuery = {
		df.filter($"jobId" === jobId and $"type" === "SandBox")
			.writeStream
			.outputMode("append")
			.format("parquet")
			.option("checkpointLocation", getCheckpointPath)
			.option("path", getOutputPath)
			.start()
	}
	
	def setInputStream(df: Option[DataFrame]): Option[sql.DataFrame] = {
		// 解析MetaData
		val mdPath = componentProperty.config("metaDataPath")
		metaData = startProcessMetaData(s"$mdPath/$jobId")
		metaData match {
			case Some(md) =>
				totalNum = md.length("length").toString.toLong
				// 将规范过后的MetaData重新写入
				writeMetaData(getMetadataPath, md)
				// 告诉pyjob有数据了
				// pushPyJob()
				// 规范化的Schema设置Stream
				df match {
					case Some(is) => Some(
						sc.column2legalWithDF("data", is.filter($"jobId" === jobId and $"type" === "SandBox"))
							.select(from_json($"data", sc.str2SqlType(write(md.schemaData))).as("data"))
							.select("data.*")
					)
					case None => logger.warn("Input Stream Is Nil"); None
				}
			case None => throw new Exception("MetaData Is Null")
		}
	}
	
	def startProcessMetaData(path: String): Option[MetaData] = {
		try {
			val content = spark.sparkContext.textFile(path)
			val m2m = BPSConcertEntry.queryComponentWithId("meta2map").get.asInstanceOf[BPSMetaData2Map]
			val sc = BPSConcertEntry.queryComponentWithId("schema convert").get.asInstanceOf[SchemaConverter]
			val primitive = m2m.list2Map(content.collect().toList)
			val convertContent = primitive ++ sc.column2legalWithMetaDataSchema(primitive)
			val schema = convertContent("schema").asInstanceOf[List[Map[String, Any]]]
			val label = convertContent.getOrElse("tag", Map.empty).asInstanceOf[Map[String, Any]]
			Some(MetaData(schema, label, Map("length" -> convertContent("length").toString.toLong)))
		} catch {
			case e: Exception => logger.error(e.getMessage, e); None
		}
	}
	
	def writeMetaData(path: String, md: MetaData): Unit = {
		hdfs.appendLine2HDFS(path, write(md.schemaData))
		hdfs.appendLine2HDFS(path, write(md.label))
		hdfs.appendLine2HDFS(path, write(md.length))
	}
	
	def pushBloodMsg(): Unit = {
		metaData match {
			case Some(md) =>
				val dataSet = new DataSet(Collections.emptyList(),
					mongoId,
					id,
					md.schemaData.map(_ ("key").toString).asInstanceOf[List[CharSequence]].asJava,
					md.label("sheetName").toString,
					totalNum,
					getOutputPath,
					"SampleData")
				val uploadEnd = new UploadEnd(mongoId, md.label("assetId").toString)
				// 血缘
				bloodStrategy.pushBloodInfo(dataSet, id, traceId)
				bloodStrategy.uploadEndPoint(uploadEnd, id, traceId)
			case _ =>
		}
	}
	
	def pushPyJob(): Unit = {
		val pythonMetaData = PythonMetaData(mongoId, "HiveTaskNone", getMetadataPath, getOutputPath, s"/jobs/$runnerId")
		// 给PythonCleanJob发送消息
		strategy.pushMsg(BPSEvents(id, traceId, msgType, pythonMetaData), isLocal = false)
	}
	
	override def createConfigDef(): ConfigDef = new ConfigDef()
	
	override val description: String = "BPSSandBoxConvertSchemaJob"
	
	case class MetaData(schemaData: List[Map[String, Any]], label: Map[String, Any], length: Map[String, Any])
	
	case class PythonMetaData(mongoId: String,
	                          noticeTopic: String,
	                          metadataPath: String,
	                          filesPath: String,
	                          resultPath: String)
}