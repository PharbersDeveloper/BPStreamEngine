package com.pharbers.StreamEngine.Others.sandbox.schema

import com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxConvertSchemaJobContainer.BPSSandBoxConvertSchemaJob
import com.pharbers.StreamEngine.Utils.Component.ComponentContext
import com.pharbers.StreamEngine.Utils.Log.BPSLogContext
import com.pharbers.StreamEngine.Utils.Session.Spark.BPSparkSession
import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor
import org.scalatest.FunSuite

class RenameTest extends FunSuite {
	test("Rename Job Test") {
		BPSLogContext.init()
//		ComponentContext.init()
		
		val job = BPSSandBoxConvertSchemaJob("id", "/test/alex2/metadata", "/test/alex2/files", "630a6-d360-491c-bf15-f04000", BPSparkSession.apply())
		job.open()
//		job.exec()
		
		ThreadExecutor.waitForShutdown()
	}
}
