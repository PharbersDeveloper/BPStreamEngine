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
	def apply(id: String,
	          spark: SparkSession,
	          is: Option[DataFrame],
	          container: BPSJobContainer): BPSSandBoxMetaDataJob =
		new BPSSandBoxMetaDataJob(id, spark, is, container)
}

class BPSSandBoxMetaDataJob(val id: String,
                            val spark: SparkSession,
                            val is: Option[sql.DataFrame],
                            val container: BPSJobContainer) extends BPStreamJob {
	type T = BPSJobStrategy
	override val strategy = null
	
	override def exec(): Unit = {
		def regJson(json:Option[Any]) = json match {
			case Some(map: Map[String, Any]) => map
		}
		inputStream = is
		inputStream match {
			case None =>
			case Some(is) =>
				println("MetaData =>>>>>>>>>>>>>>>>>>>>>>")
				val traceId = "da0fb-c055-4d27-9d1a-fc989"
				val tmp = is.collect().map(x =>
					x.getAs[String]("value").replaceAll("""\\"""", "")).toList
				val schema = tmp.head
				val length = regJson(JSON.parseFull(tmp.last)).getOrElse("length", 0).toString.toDouble.toInt
				BPFileMeta2Mongo(traceId, Nil, schema, length).SchemaData()
//				post(s"""{"traceId": "$traceId"}""", "application/json")
		
		}
	}
	
	override def close(): Unit = {
		super.close()
		container.finishJobWithId(id)
	}
	
	def post(body: String, contentType: String): Unit = {
		val conn = new URL("http://192.168.100.116:36416/v0/Stream2HDFSFinish").openConnection.asInstanceOf[HttpURLConnection]
		val postDataBytes = body.getBytes(StandardCharsets.UTF_8)
		conn.setRequestMethod("POST")
		conn.setRequestProperty("Content-Type", contentType)
		conn.setRequestProperty("Content-Length", String.valueOf(postDataBytes.length))
		conn.setConnectTimeout(60000)
		conn.setReadTimeout(60000)
		conn.setDoOutput(true)
		conn.getOutputStream.write(postDataBytes)
		conn.getResponseCode
	}
}
