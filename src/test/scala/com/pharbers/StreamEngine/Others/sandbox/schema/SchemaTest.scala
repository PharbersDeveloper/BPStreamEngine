package com.pharbers.StreamEngine.Others.sandbox.schema

import java.util.UUID

import com.pharbers.StreamEngine.Jobs.SandBoxJob.ConvertMAX5.BPSConvertMAX5JobContainer
import com.pharbers.StreamEngine.Jobs.SandBoxJob.FileMeta2Mongo.BPSMongo.MongoTrait
import com.pharbers.StreamEngine.Utils.Component.ComponentContext
import com.pharbers.StreamEngine.Utils.Session.Spark.BPSparkSession
import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor
import org.scalatest.FunSuite

class SchemaTest extends FunSuite with MongoTrait {
	import com.mongodb.casbah.Imports._
	test("to Clock data数据") {
		ComponentContext.init()
		val id = "ff89f6cf-7f52-4ae1-a5ec-2609169b3995"//UUID.randomUUID().toString
		println(s"=========> uuid =>$id")
//		val spark = BPSparkSession()
		val metaDataPath = "/test/streamingV2/0829b025-48ac-450c-843c-6d4ee91765ca/metadata"
		val sampleDataPath = "/test/streamingV2/0829b025-48ac-450c-843c-6d4ee91765ca/files/jobId="
		
//		val jobId = "23d78-4083-422a-9fdc-b227f1"
//		val job = BPSConvertMAX5JobContainer(id, metaDataPath, sampleDataPath, jobId, "Pfizer_1701_1712_GYC.csv" ,spark)
//		job.open()
//		job.exec()
		
		val result = queryAll("FileMetaDatum")
		println(result.size)
		val finalResult = result.flatMap{ r =>
			r.get("job-id").asInstanceOf[BasicDBList].toList.flatMap{ l =>
				Map(r.getAsOrElse[String]("name", "") -> l.toString)
			}
		}
		println(finalResult.size)
		
//		val head = finalResult.head
//		val tail = finalResult.tail
//		val job = BPSConvertMAX5JobContainer(id, metaDataPath, sampleDataPath, head._2, head._1, tail, spark)
//		job.open()
//		job.exec()
		
		ThreadExecutor.waitForShutdown()
	}
}
