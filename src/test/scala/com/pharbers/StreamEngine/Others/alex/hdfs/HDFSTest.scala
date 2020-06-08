package com.pharbers.StreamEngine.Others.alex.hdfs

import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Strategy.Schema.{BPSMetaData2Map, SchemaConverter}
import com.pharbers.StreamEngine.Utils.Strategy.hdfs.BPSHDFSFile
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import com.pharbers.StreamEngine.Utils.Strategy.s3a.BPS3aFile
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
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
	
	test("递归读取文件") {
		val hdfsfile: BPSHDFSFile = BPSConcertEntry.queryComponentWithId("hdfs").get.asInstanceOf[BPSHDFSFile]
		val s3aFile: BPS3aFile = BPSConcertEntry.queryComponentWithId("s3a").get.asInstanceOf[BPS3aFile]
		
		hdfsfile.recursiveFiles("hdfs://starLord:8020//jobs/5ebb72cac46f040c39045027/BPSPythonJobContainer/5256f6d3-9359-4977-ba82-6a32467d7b1d/checkpoint") match {
			case Some(r) =>
				r.foreach { x =>
					s3aFile.copyHDFSFiles(s"s3a://ph-stream${x.path}", x.name, x.input)
				}
				r.head.fs.close()
			case _ =>
		}
	}
	
	test("spark streaming 读取S3 parquet") {
		val spark: SparkSession = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession]
		val path = "s3a://ph-stream/jobs/runId_5ec4c985118a4a6530a42fd6/BPSOssPartitionJob/jobId_8852934e-39fd-482a-a206-1f6832fc3dfa/contents/jobId=002bf498-ded1-4122-9206-35d77253cd290/"
		val reading = spark.readStream
			.schema(StructType(
				StructField("traceId", StringType) ::
					StructField("type", StringType) ::
					StructField("data", StringType) ::
					StructField("timestamp", TimestampType) :: Nil
			)).parquet(path)
		val query = reading.writeStream.outputMode("append").format("console").start()
		query.awaitTermination()
	}
}

