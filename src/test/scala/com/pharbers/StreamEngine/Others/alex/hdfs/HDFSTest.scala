package com.pharbers.StreamEngine.Others.alex.hdfs

import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Strategy.Schema.{BPSMetaData2Map, SchemaConverter}
import com.pharbers.StreamEngine.Utils.Strategy.hdfs.BPSHDFSFile
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

import scala.io.Source

class HDFSTest extends FunSuite {
	test("HDFS Append Test") {
		//		try {
		//			1 to 100 foreach { x =>
		//				BPSHDFSFile.appendLine2HDFS(s"/jobs/test/$x", "Fuck")
		//			}
		//		} catch {
		//			case e: Exception => e.printStackTrace()
		//		}
	}
	
	test("Read Parquet With Path File") {
		val spark = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession]
		import scala.io.Source
		
		val source = Source.fromFile("/Users/qianpeng/Desktop/files.txt", "UTF-8")
		val lines = source.getLines().toArray
		source.close()
		lines.foreach { x =>
			val parquetUrl = s"$x/contents"
			val metaDataUrl = s"$x/metadata"
			val parquetReading = spark.read.parquet(parquetUrl)
			val content = spark.sparkContext.textFile(metaDataUrl)
			val m2m = BPSConcertEntry.queryComponentWithId("meta2map").get.asInstanceOf[BPSMetaData2Map]
			val sc = BPSConcertEntry.queryComponentWithId("schema convert").get.asInstanceOf[SchemaConverter]
			val primitive = m2m.list2Map(content.collect().toList)
			val convertContent = primitive ++ sc.column2legalWithMetaDataSchema(primitive)
			val metaDataNum = convertContent("length").toString.toLong
			
			val count = parquetReading.count()
			if (count < metaDataNum) {
				println(parquetUrl)
				println(count)
			}
			
		}
	}
	
	test("Read Parquet With Path") {
		val spark = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession]
		val hdfsUrl = s"/common/projects/max/Sankyo/prod_mapping"
		val reading = spark.read.parquet(hdfsUrl)
		reading.show()
		val count = reading.count()
		println(count)
	}
}

