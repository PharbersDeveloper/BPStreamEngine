package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxMetaDataJob


import com.pharbers.StreamEngine.Jobs.SandBoxJob.FileMeta2Mongo.BPSMongo.BPFileMeta2Mongo
import org.apache.spark.sql.SparkSession
import scala.util.parsing.json._

// TODO: 需要重构拓展不好，所有血缘入库的MetaData地方都应该走这个job
object BPSSandBoxMetaDataJob {
	def apply(path: String,
	          jobId: String,
	          spark: SparkSession): BPSSandBoxMetaDataJob =
		new BPSSandBoxMetaDataJob(path, jobId, spark)
}

class BPSSandBoxMetaDataJob(path: String, jobId: String, spark: SparkSession) {
	def regJson(json: Option[Any]): Map[String, Any] = json match {
		case Some(map: Map[String, Any]) => map
	}
	
	def exec(): Unit = {
		val metaData: List[String] = spark.sparkContext.textFile(s"$path/$jobId").collect().toList.map(x =>
			x.replaceAll("""\\"""", ""))
		val schema: String = metaData.head
		val length: Int = regJson(JSON.parseFull(metaData.last)).getOrElse("length", 0).toString.toDouble.toInt
		BPFileMeta2Mongo(jobId, Nil, schema, length).SchemaData()
	}
}
