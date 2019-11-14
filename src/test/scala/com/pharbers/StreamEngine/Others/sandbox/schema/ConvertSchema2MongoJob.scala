package com.pharbers.StreamEngine.Others.sandbox.schema

import com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxMetaDataJob.BPSSandBoxMetaDataJob
import com.pharbers.StreamEngine.Utils.Log.BPSLogContext
import com.pharbers.StreamEngine.Utils.Session.Spark.BPSparkSession
import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor
import org.scalatest.FunSuite

class ConvertSchema2MongoJob extends FunSuite {
	test("") {
		BPSLogContext.init()
		val job = BPSSandBoxMetaDataJob("/workData/streamingV2/67a508c0-59ca-409f-b74b-223f0241afe2/metadata", "db7c8-b275-4d72-adc3-aa4070",  BPSparkSession.apply())
		job.exec()
		
		ThreadExecutor.waitForShutdown()
	}
	
}
