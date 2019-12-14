package com.pharbers.StreamEngine.Others.sandbox.schema

import com.pharbers.StreamEngine.Jobs.SandBoxJob.SchemaConverter
import com.pharbers.StreamEngine.Utils.Schema.Spark.BPSMetaData2Map
import com.pharbers.StreamEngine.Utils.Session.Spark.BPSparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, regexp_replace}
import org.scalatest.FunSuite
import org.json4s._
import org.json4s.jackson.Serialization.write


class RenameColumnTest extends FunSuite{
	test("rename column") {
//		val jsonStr = """[{"key": "城市","type": "String"},{"key": "城市","type": "String"},{"key": "年月","type": "String"}]"""
//
//		SchemaConverter.renameColumn(jsonStr)
		val spark = BPSparkSession()
		
		import org.apache.spark.sql.functions._
		
		def replaceJsonStr(json: String) = {
			""
		}
		
		val udf_rep = udf(replaceJsonStr _)
		
		val reading = spark.read
			.load("/jobs/3ca35e29-3198-43ea-b8f6-c73e14a4e1b8/353d9d01-3715-48d7-89e4-ebd042d45faa/contents/jobId=fe17d1ca-2f44-4df7-943d-77e75cd780cc1") //文件的路径
		reading.select("data").withColumn("data", udf_rep(col("data"))).show(false)
//		reading.select("data").withColumn("data", regexp_replace(col("data") , """(?<=#)(,)(?=")""", "")).show(false)
		println(reading.count())
//		val metaData = spark.sparkContext.textFile("/jobs/49324e09-d7f9-4455-935a-b33c0d27d59e/aef91ccd-469d-46fb-b2b7-c2009f9a335a/metadata/b3df62cd-31c6-4fe6-b06c-060a7b9d94c21")
//		println(writeMetaData(metaData, ""))
	}
	
	def writeMetaData(metaData: RDD[String], path: String): (String, List[CharSequence], String, Int, String) = {
		try {
			val primitive = BPSMetaData2Map.list2Map(metaData.collect().toList)
			val convertContent = primitive ++ SchemaConverter.column2legalWithMetaDataSchema(primitive)
			
			implicit val formats: DefaultFormats.type = DefaultFormats
			val schema = write(convertContent("schema").asInstanceOf[List[Map[String, Any]]])
			val colNames = convertContent("schema").asInstanceOf[List[Map[String, Any]]].map(_ ("key").toString)
			val tabName = convertContent.
				getOrElse("tag", Map.empty).
				asInstanceOf[Map[String, Any]].
				getOrElse("sheetName", "").toString
			
			val assetId = convertContent.
				getOrElse("tag", Map.empty).
				asInstanceOf[Map[String, Any]].
				getOrElse("assetId", "").toString
			
			convertContent.foreach { x =>
				println(write(x._2))
			}
			
			(schema, colNames, tabName, convertContent("length").toString.toInt, assetId)
			
			
			//			val metaDataDF = SchemaConverter.column2legalWithDF("MetaData", metaData.toDF("MetaData"))
			//			val contentMap = BPSMetaData2Map.
			//				list2Map(metaDataDF.select("MetaData").collect().toList.map(_.getAs[String]("MetaData")))
			//			implicit val formats: DefaultFormats.type = DefaultFormats
			//			val schema  = write(contentMap("schema").asInstanceOf[List[Map[String, Any]]])
			//			metaDataDF.collect().foreach(x => BPSHDFSFile.appendLine2HDFS(path, x.getAs[String]("MetaData")))
			//
			//			val colNames =  contentMap("schema").asInstanceOf[List[Map[String, Any]]].map(_("key").toString)
			//			val tabName = contentMap.
			//				getOrElse("tag", Map.empty).
			//				asInstanceOf[Map[String, Any]].
			//				getOrElse("sheetName", "").toString
			//
			//			val assetId = contentMap.
			//				getOrElse("tag", Map.empty).
			//				asInstanceOf[Map[String, Any]].
			//				getOrElse("assetId", "").toString
			//
			//			(schema, colNames, tabName, contentMap("length").toString.toInt, assetId)
		} catch {
			case e: Exception =>
				// TODO: 处理不了发送重试
				("", Nil, "", 0, "")
		}
	}
		
}
