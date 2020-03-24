package com.pharbers.StreamEngine.Others.sandbox.schema

import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Strategy.hdfs.BPSHDFSFile
import com.pharbers.StreamEngine.Utils.Strategy.Schema.{BPSMetaData2Map, SchemaConverter}
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import com.pharbers.util.log.PhLogable
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import org.json4s._
import org.json4s.jackson.Serialization.write
import org.apache.spark.sql.functions._

import scala.io.Source

class RenameColumnTest extends FunSuite with PhLogable {
//	test("rename column") {
//
//		val source = Source.fromFile("/Users/qianpeng/Desktop/dfs.txt")
//		val lines = source.getLines().toList
//		source.close()
//
//		val spark = BPSparkSession()
//		lines.foreach { line =>
//			val tmp = line.split(",")
//			try {
//				val reading = spark.read
//					.load(tmp(2)) //文件的路径
//				val count = reading.count()
//				val fileCount = tmp(3).toLong
//				println(s"fileSource ==> ${tmp(2)} ====> $fileCount <=========> $count")
//				if (fileCount != count) {
//					logger.info(s"""{"assetId": "${tmp(0)}", "dfId": "${tmp(1)}"}""")
//				}
//			} catch {
//				case e: Exception =>
//					logger.error(s"fileSource ==> ${tmp(2)}")
//			}
//
//		}
//
//	}

	test("test") {
		val spark = BPSparkSession(null)
		val metaData = spark.sparkContext.textFile("/jobs/cedb4c1c-d8f6-44b6-900e-3e1dd274f7cd/e46aa5b8-006a-4737-8569-76bb80403298/metadata/be9bfbf5-117a-41b8-873c-3c9182f2bc951")
		val (schemaData, colNames, tabName, length, assetId) =
			writeMetaData(metaData, s"")
	}

	def writeMetaData(metaData: RDD[String], path: String): (String, List[CharSequence], String, Int, String) = {
		try {
			val m2m = BPSConcertEntry.queryComponentWithId("meta2map").asInstanceOf[BPSMetaData2Map]
			val primitive = m2m.list2Map(metaData.collect().toList)
			val convertContent = primitive ++ SchemaConverter.column2legalWithMetaDataSchema(primitive)

			implicit val formats: DefaultFormats.type = DefaultFormats
			val schema  = write(convertContent("schema").asInstanceOf[List[Map[String, Any]]])
			val colNames =  convertContent("schema").asInstanceOf[List[Map[String, Any]]].map(_("key").toString)
			val tabName = convertContent.
				getOrElse("tag", Map.empty).
				asInstanceOf[Map[String, Any]].
				getOrElse("sheetName", "").toString

			val assetId = convertContent.
				getOrElse("tag", Map.empty).
				asInstanceOf[Map[String, Any]].
				getOrElse("assetId", "").toString

			convertContent.foreach { x =>
				if (x._1 == "length") {
					println(s"""{"length": ${x._2}}""")
				} else {
					println(write(x._2))
				}

//				BPSHDFSFile.appendLine2HDFS(path, write(x._2))
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
				logger.error(e.getMessage)
				("", Nil, "", 0, "")
		}

	}

	test("read hive schema and type") {
		val spark = BPSparkSession(null)
		val result = spark.sql("SELECT * FROM chc limit 10")
		result.show()
		result.printSchema()
	}
}

