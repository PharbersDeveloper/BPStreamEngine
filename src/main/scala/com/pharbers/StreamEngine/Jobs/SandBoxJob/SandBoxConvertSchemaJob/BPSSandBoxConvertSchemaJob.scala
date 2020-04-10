package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxConvertSchemaJob

import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Event.BPSTypeEvents
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPJobLocalListener
import com.pharbers.StreamEngine.Utils.Job.{BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Utils.Strategy.JobStrategy.BPSCommonJobStrategy
import com.pharbers.StreamEngine.Utils.Strategy.Schema.{BPSMetaData2Map, SchemaConverter}
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.msgMode.SparkQueryEvent
import com.pharbers.StreamEngine.Utils.Strategy.hdfs.BPSHDFSFile
import com.pharbers.kafka.schema.{BPJob, DataSet, UploadEnd}
import org.apache.kafka.common.config.ConfigDef
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization._


case class BPSSandBoxConvertSchemaJob(container: BPSJobContainer,
                                      componentProperty: Component2.BPComponentConfig) extends BPStreamJob {
	
	type T = BPSCommonJobStrategy
	override val strategy = BPSCommonJobStrategy(componentProperty.config, configDef)
	override val id: String = componentProperty.id // 本身Job的id
	val jobId: String = strategy.getJobId // componentProperty config中的job Id
	val runnerId: String = BPSConcertEntry.runner_id // Runner Id
	val spark: SparkSession = strategy.getSpark
	val sc: SchemaConverter = strategy.getSchemaConverter
	val hdfs: BPSHDFSFile = strategy.getHdfsFile
	var totalNum: Long = 0
	implicit val formats: DefaultFormats.type = DefaultFormats
	
	import spark.implicits._
	
	override def open(): Unit = {
		inputStream = container.inputStream
		setInputStream(inputStream)
	}
	
	override def exec(): Unit = inputStream match {
		case Some(is) => {
			val query = startProcessParquet(is)
			outputStream = outputStream :+ query
			
			val rowNumListener =
				BPJobLocalListener[SparkQueryEvent](null, List(s"spark-${query.id.toString}-progress"))(_ => {
					val cumulative = query.recentProgress.map(_.numInputRows).sum
					println(s"cumulative num $cumulative")
					if (cumulative >= totalNum) {
						query.stop()
						this.close()
					}
				})
			rowNumListener.active(null)
		}
		case None => ???
	}
	
	override def close(): Unit = {
		println("Job =====> Closed")
		super.close()
		container.finishJobWithId(id)
	}
	
	def startProcessParquet(df: DataFrame): StreamingQuery = {
		df.filter($"jobId" === jobId and $"type" === "SandBox")
			.writeStream
			.outputMode("append")
			.format("parquet")
			.option("checkpointLocation", s"/jobs/$runnerId/$id/checkpoint")
			.option("path", s"/jobs/$runnerId/$id/contents")
			.start()
	}
	
	def setInputStream(df: Option[DataFrame]): Unit = {
		try {
			//TODO 串联执行会更好,@Alex留给自己
			// 解析MetaData
			val metaDataPath = componentProperty.config("metaDataPath")
			val metaData = startProcessMetaData(s"$metaDataPath/$jobId")
			totalNum = metaData.length("length").toString.toLong
			// 将规范过后的MetaData重新写入
			writeMetaData(s"/jobs/$runnerId/$id/metadata", metaData)
			// 规范化的Schema设置Stream
			df match {
				case Some(is) => {
					is.filter($"jobId" === "" and $"type" === "SandBox")
					inputStream = Some(
						sc.column2legalWithDF("data", is)
							.select(from_json($"data", sc.str2SqlType(write(metaData.schemaData))).as("data"))
							.select("data.*")
					)
				}
				case None => logger.warn("Input Stream Is Nil")
			}
			
		} catch {
			case e: Exception =>
				logger.error(e.getMessage)
				this.close()
		}
		
	}
	
	def startProcessMetaData(path: String): MetaData = {
		val content = spark.sparkContext.textFile(path)
		val m2m = BPSConcertEntry.queryComponentWithId("meta2map").get.asInstanceOf[BPSMetaData2Map]
		val sc = BPSConcertEntry.queryComponentWithId("schema convert").get.asInstanceOf[SchemaConverter]
		val primitive = m2m.list2Map(content.collect().toList)
		val convertContent = primitive ++ sc.column2legalWithMetaDataSchema(primitive)
		val schema = convertContent("schema").asInstanceOf[List[Map[String, Any]]]
		val label = convertContent.getOrElse("tag", Map.empty).asInstanceOf[Map[String, Any]]
		MetaData(schema, label, Map("length" -> convertContent("length").toString.toLong))
	}
	
	def writeMetaData(path: String, md: MetaData): Unit = {
		hdfs.appendLine2HDFS(path, write(md.schemaData))
		hdfs.appendLine2HDFS(path, write(md.label))
		hdfs.appendLine2HDFS(path, write(md.length))
	}
	
	override def createConfigDef(): ConfigDef = new ConfigDef()
	
	override val description: String = "BPSSandBoxConvertSchemaJob"
	
	case class MetaData(schemaData: List[Map[String, Any]], label: Map[String, Any], length: Map[String, Any])
	
}