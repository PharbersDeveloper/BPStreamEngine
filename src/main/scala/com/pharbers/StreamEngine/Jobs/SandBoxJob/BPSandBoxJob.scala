package com.pharbers.StreamEngine.Jobs.SandBoxJob

import com.pharbers.StreamEngine.Jobs.SandBoxJob.FileMeta2Mongo.BPSMongo.BPFileMeta2Mongo
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSJobStrategy
import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

//object BPSandBoxJob {
//	def apply(id: String,
//	          spark: SparkSession,
//	          traceId: String,
//	          jobId: String,
//	          line: String,
//	          `type`: String): BPSandBoxJob = new BPSandBoxJob(id, spark, traceId, jobId, line, `type`)
//}
//
//class BPSandBoxJob (val id: String,
//                    val spark: SparkSession,
//                    traceId: String,
//                    jobId: String,
//                    line: String,
//                    `type`: String) extends BPStreamJob {
//	type T = BPSJobStrategy
//	override val strategy = null
//
//	override def exec(): Unit = {
//		`type` match {
//			case "SandBox-Schema" =>
//				println(s"Fucking SandBox-Schema Data => $line")
//
//			case "SandBox-Length" =>
//				println(s"Fucking SandBox-Length Data => $line")
//			case "SandBox" =>
//				println(s"Fucking SandBox Data => $line")
//			case _ =>
//		}
//	}
//
//	override def close(): Unit = {
//		super.close()
//	}
//}

object BPSandBoxJob {
	def apply(id: String,
		      spark: SparkSession,
		      inputStream: Option[sql.DataFrame],
		      container: BPSJobContainer): BPSandBoxJob = new BPSandBoxJob(id, spark, inputStream, container)
}

class BPSandBoxJob (val id: String,
                    val spark: SparkSession,
                    val is: Option[sql.DataFrame],
                    val container: BPSJobContainer) extends BPStreamJob {
	type T = BPSJobStrategy
	override val strategy = null
	import spark.implicits._
	
	override def exec(): Unit = {
		inputStream = is
		inputStream match {
			case None =>
			case Some(is) if is.filter($"type" === "SandBox").count() > 0 =>
//				is.take(5).foreach(r => println(r))
				// 2种过滤掉其他字段只保留traceId和SandBox 明天测试
//				is.selectExpr()
//				is.drop("")
				val traceId = is.first().getAs[String]("traceId")
				val sampleData = is.toJSON.take(5).toList
				BPFileMeta2Mongo(traceId, sampleData, "", 0).MetaData()
				
			case Some(is) =>
				is.show() // metadata是个text具体结构测试的时候打印出来看看

		}
	}
	
	override def close(): Unit = {
		super.close()
		container.finishJobWithId(id)
	}
}