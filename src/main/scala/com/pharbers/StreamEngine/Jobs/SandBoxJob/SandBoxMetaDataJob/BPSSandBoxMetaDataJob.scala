package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxMetaDataJob


import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets
import org.json4s.jackson.Serialization._
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats


// TODO: 需要重构拓展不好，所有血缘入库的MetaData地方都应该走这个job
object BPSSandBoxMetaDataJob {
	def apply(path: String,
	          jobId: String,
	          spark: SparkSession): BPSSandBoxMetaDataJob =
		new BPSSandBoxMetaDataJob(path, jobId, spark)
}
class BPSSandBoxMetaDataJob(path: String, jobId: String, spark: SparkSession) {
	
	def exec(): Unit = {
		val schemaKey = "[{"
		val lengthKey = """{"length":"""
		val labelsKey = """{"label":"""
		val metaData: List[String] = spark.sparkContext.textFile(s"$path/$jobId").collect().toList.map(x =>
			x.replaceAll("""\\"""", ""))
		
		val schemaStr = findByKey(metaData, schemaKey)
		implicit val formats: DefaultFormats.type = DefaultFormats
		val columnName = read[List[Map[String, String]]](schemaStr).map(_("key"))
		
		val length = findByKey(metaData, lengthKey)
			.split(":").last
			.replaceAll("_", "")
			.replaceAll("}", "").trim.toLong
		
		val labelStr = findByKey(metaData, labelsKey)
		val sheetName = read[Map[String, Any]](labelStr)
		// TODO 话说为啥我要这么拼接呢，搞个case class不就好了，通了之后再搞吧
		val json = s"""{"columnName":${write(columnName)},"length":"$length","jobId":"$jobId","path":"$path/$jobId","sheetName":"${sheetName("sheetName")}"}"""
		
		post(json, "application/json")
	}
	
	def post(body: String, contentType: String): Unit = {
		val conn = new URL("http://192.168.100.116:8080/updateInfoWithJobId").openConnection.asInstanceOf[HttpURLConnection]
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
	
	def findByKey(listStr: List[String], key: String): String = {
		listStr.find(x => x.contains(key)) match {
			case None => ""
			case Some(x) => x
		}
	}
}
