package com.pharbers.StreamEngine.Others.sandbox.schema

import com.pharbers.StreamEngine.Jobs.SandBoxJob.FileMeta2Mongo.BPSMongo.MongoTrait
import com.pharbers.StreamEngine.Utils.Component.ComponentContext
import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor
import org.scalatest.FunSuite

class SchemaTest extends FunSuite with MongoTrait {
	test("to Clock data数据") {
		ComponentContext.init()
		
		ThreadExecutor.waitForShutdown()
	}
}
