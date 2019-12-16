package com.pharbers.StreamEngine.Others.sandbox.schema

import com.pharbers.StreamEngine.Utils.Schema.Spark.{BPSMetaData2Map, SchemaConverter}
import com.pharbers.StreamEngine.Utils.Session.Spark.BPSparkSession
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import org.json4s._
import org.json4s.jackson.Serialization.write
import org.apache.spark.sql.functions._


class RenameColumnTest extends FunSuite {
	test("rename column") {
		val spark = BPSparkSession()
		val reading = spark.read
			.load("/user/alex/jobs/3cab7779-7c5c-4b6b-ac87-60a1f5ddbca8/6722c9a2-b3d2-4bbf-a35b-2e88c7dbe007/contents") //文件的路径
		reading.show(false)
		println(reading.count())
//		val str = """aa bb,cc,{dd},(ee),(ff),{gg}"""
//		println(str)
//		println(str.replaceAll("""[,{}()\s\t\n]""", ""))
	}
	
}

