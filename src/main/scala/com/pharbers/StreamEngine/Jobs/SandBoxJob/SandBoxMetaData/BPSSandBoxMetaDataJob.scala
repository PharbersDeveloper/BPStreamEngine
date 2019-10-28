package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxMetaData

import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets

import com.pharbers.StreamEngine.Jobs.SandBoxJob.FileMeta2Mongo.BPSMongo.BPFileMeta2Mongo
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSJobStrategy
import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.parsing.json._

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
		val metaData: List[String] = spark.sparkContext.textFile(s"$path$jobId").collect().toList.map(x =>
			x.replaceAll("""\\"""", ""))
		val schema: String = metaData.head
		val length: Int = regJson(JSON.parseFull(metaData.last)).getOrElse("length", 0).toString.toDouble.toInt
		println(schema)
		println(length)
//		BPFileMeta2Mongo(jobId, Nil, schema, length).SchemaData()
	}
	
	
//	def post(body: String, contentType: String): Unit = {
//		val conn = new URL("http://192.168.100.116:36416/v0/Stream2HDFSFinish").openConnection.asInstanceOf[HttpURLConnection]
//		val postDataBytes = body.getBytes(StandardCharsets.UTF_8)
//		conn.setRequestMethod("POST")
//		conn.setRequestProperty("Content-Type", contentType)
//		conn.setRequestProperty("Content-Length", String.valueOf(postDataBytes.length))
//		conn.setConnectTimeout(60000)
//		conn.setReadTimeout(60000)
//		conn.setDoOutput(true)
//		conn.getOutputStream.write(postDataBytes)
//		conn.getResponseCode
//	}
}
