package com.pharbers.StreamEngine.Others.sandbox.schema

import com.pharbers.StreamEngine.Jobs.SandBoxJob.SchemaConverter
import com.pharbers.StreamEngine.Others.clock.ReadParquetScript.spark
import com.pharbers.StreamEngine.Utils.Session.Spark.BPSparkSession
import org.scalatest.FunSuite

class RenameColumnTest extends FunSuite{
	test("rename column") {
//		val jsonStr = """[{"key": "城市","type": "String"},{"key": "城市","type": "String"},{"key": "年月","type": "String"}]"""
//
//		SchemaConverter.renameColumn(jsonStr)
		val spark = BPSparkSession()
		
		val reading = spark.read
			.load("/user/alex/jobs/94350fa8-420e-4f5f-8f1e-467627aafec3/ff94c3bd-1887-4800-a5f4-43a292869008/contents/6f6b8e34-88df-4bef-a930-20197ccd6ef0") //文件的路径
		reading.show(false)
		println(reading.count())
	}
}
