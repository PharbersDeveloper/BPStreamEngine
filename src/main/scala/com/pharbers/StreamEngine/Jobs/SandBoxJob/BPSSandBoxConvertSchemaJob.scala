package com.pharbers.StreamEngine.Jobs.SandBoxJob

import com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxJobContainer.BPSSandBoxJobContainer
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPJobLocalListener
import com.pharbers.StreamEngine.Utils.Job.Status.BPSJobStatus
import com.pharbers.StreamEngine.Utils.Job.{BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Utils.Module.bloodModules.{BloodModel, BloodModel2, DataMartTagModel, UploadEndModel}
import com.pharbers.StreamEngine.Utils.Strategy.Blood.BPSSetBloodStrategy
import com.pharbers.StreamEngine.Utils.Strategy.JobStrategy.BPSCommonJobStrategy
import com.pharbers.StreamEngine.Utils.Strategy.Schema.{BPSMetaData2Map, SchemaConverter}
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.msgMode.SparkQueryEvent
import com.pharbers.StreamEngine.Utils.Strategy.hdfs.BPSHDFSFile
import com.pharbers.StreamEngine.Utils.Strategy.s3a.BPS3aFile
import org.apache.kafka.common.config.ConfigDef
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write
import org.mongodb.scala.bson.ObjectId

case class BPSSandBoxConvertSchemaJob(container: BPSJobContainer, input: Option[DataFrame],
                                      componentProperty: Component2.BPComponentConfig) extends BPStreamJob {
	
	type T = BPSCommonJobStrategy
	override val strategy: BPSCommonJobStrategy = BPSCommonJobStrategy(componentProperty.config, configDef)
	val bloodStrategy: BPSSetBloodStrategy = new BPSSetBloodStrategy(componentProperty.config)
	override val id: String = componentProperty.id // 本身Job的id
	val jobId: String = strategy.getJobId // componentProperty config中的job Id
	val runnerId: String = BPSConcertEntry.runner_id // Runner Id
	val s3aFile: BPS3aFile = BPSConcertEntry.queryComponentWithId("s3a").get.asInstanceOf[BPS3aFile]
	val traceId: String = componentProperty.args.head
	val msgType: String = componentProperty.args.last
	val spark: SparkSession = strategy.getSpark
	val sc: SchemaConverter = strategy.getSchemaConverter
	val hdfs: BPSHDFSFile = strategy.getHdfsFile
	var totalNum: Long = 0
	val mongoId: String = new ObjectId().toString
	var metaData: Option[MetaData] = None
	implicit val formats: DefaultFormats.type = DefaultFormats
	
	import spark.implicits._
	
	override def open(): Unit = {
		inputStream = setInputStream(input)
	}
	
	override def exec(): Unit = inputStream match {
		case Some(is) => {
			val query = startProcessParquet(is)
			outputStream = outputStream :+ query
			
			val rowNumListener =
				BPJobLocalListener[SparkQueryEvent](null, List(s"spark-${query.id.toString}-progress"))(x => {
					logger.info(s"listener hit query ${x.date.id}")
					checkQuery()
				})
			rowNumListener.active(null)
			checkQuery()
			listeners = listeners :+ rowNumListener
		}
		case None => ???
	}
	
	override def close(): Unit = {
		logger.info("Job =====> Closed")
		
		metaData match {
			case Some(md) =>
				pushBloodMsg(BPSJobStatus.End.toString, md)
				bloodStrategy.setMartTags(DataMartTagModel(md.label("assetId").toString, md.label("tag").toString), id, traceId)
			case _ =>
		}
		val bpsEvents = BPSEvents("", "", s"SandBoxJobEnd", "")
		strategy.pushMsg(bpsEvents, isLocal = false)
		super.close()
		container.finishJobWithId(id)
	}
	
	def startProcessParquet(df: DataFrame): StreamingQuery = {
		val partitionNum = math.ceil(totalNum / 100000D).toInt
		df.filter($"type" === "SandBox")
			//todo: 这儿直接repartition为了控制储存的每个文件的数据量，但是一个批次读入的数据可能不是全部数据。而计算分片是按照总数据量计算的，所以可能会导致文件过小
			.repartition(partitionNum)
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
//				pushPyJob(md)
				// 规范化的Schema设置Stream
				df match {
					case Some(is) => Some(
						sc.column2legalWithDF("data", is.filter($"type" === "SandBox"))
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
			case e: Exception =>
				logger.error(s"${e.getMessage} jobId ===> $id, upper job meta data path ====> $path", e); None
		}
	}
	
	def writeMetaData(path: String, md: MetaData): Unit = {
		s3aFile.appendLine(path, write(md.schemaData))
		s3aFile.appendLine(path, write(md.label))
		s3aFile.appendLine(path, write(md.length))
	}
	
	def pushBloodMsg(status: String, metaData: MetaData): Unit = {
		val bloodModel = BloodModel(mongoId, metaData.label("assetId").toString, Nil,
			id, metaData.schemaData.map(_ ("key").toString),
			metaData.label("sheetName").toString, totalNum,
			getOutputPath, "schemaJob", status)
//		val bloodModel = BloodModel2(
//			jobId = "jobId", // TODO 还差JobId
//			columnNames = metaData.schemaData.map(_ ("key").toString),
//			tabName = metaData.label("sheetName").toString,
//			length = totalNum,
//			url = getOutputPath,
//			description = "schemaJob",
//			status = status)
		
		
		// 血缘
		bloodStrategy.pushBloodInfo(bloodModel, id, traceId)
	}
	
	def pushPyJob(metaData: MetaData): Unit = {
		val pythonMetaData = PythonMetaData(mongoId, metaData.label("assetId").toString, "HiveTaskNone", getMetadataPath, getOutputPath, s"hdfs://spark.master:8020//jobs/runId_$runnerId")
		// 给PythonCleanJob发送消息
		strategy.pushMsg(BPSEvents(id, traceId, msgType, pythonMetaData), isLocal = false)
	}
	
	def checkQuery(): Unit = {
		val query = outputStream.head
		val cumulative = query.recentProgress.map(_.numInputRows).sum
		logger.info(s"cumulative num $cumulative, id: $id, query: ${query.id.toString}")
		if (cumulative >= totalNum) {
			this.close()
		}
	}
	
	override def createConfigDef(): ConfigDef = new ConfigDef()
	
	override val description: String = "BPSSandBoxConvertSchemaJob"
	
	case class MetaData(schemaData: List[Map[String, Any]], label: Map[String, Any], length: Map[String, Any])
	
	case class PythonMetaData(mongoId: String,
	                          assetId: String,
	                          noticeTopic: String,
	                          metadataPath: String,
	                          filesPath: String,
	                          resultPath: String)
	
	
	case class PythonMetaData2(jobId: String,
	                          noticeTopic: String,
	                          metadataPath: String,
	                          filesPath: String,
	                          resultPath: String)
	
}