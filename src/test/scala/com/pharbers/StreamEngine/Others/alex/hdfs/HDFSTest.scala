package com.pharbers.StreamEngine.Others.alex.hdfs

import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
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
			val hdfsUrl = s"/jobs/5e8f1871684d707c34f40b19/$x/contents"
			val reading = spark.read.parquet(hdfsUrl)
			//			reading.show()
			val count = reading.count()
			if (count < 145) {
				println(x)
				println(count)
			}
			
		}
	}
	
	test("Read Parquet With Path") {
		val spark = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession]
		val hdfsUrl = s"/jobs/5e95b3801d45316c2831b98b/BPSSandBoxConvertSchemaJob/422934ad-be3b-41e9-a349-cc105e0d39d2/contents"
		val reading = spark.read.parquet(hdfsUrl)
		reading.show()
		val count = reading.count()
		println(count)
	}
}

