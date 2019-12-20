package com.pharbers.StreamEngine.Others.sandbox.schema

import com.pharbers.StreamEngine.Utils.Session.Spark.BPSparkSession
import org.scalatest.FunSuite

import scala.io.Source

class ReadLogTest extends FunSuite {
	test("read log") {
		val source = Source.fromFile("/Users/qianpeng/Desktop/app.log")
		source.getLines().toList.foreach { line =>
			if (line.indexOf("***") > -1 && line.lastIndexOf("***") > -1) {
				val jobId = line.substring(line.indexOf("***") + 3, line.lastIndexOf("***"))
				println(jobId)
			}
		}
		source.close()
	}
	
	test("read exec count") {
		val source = Source.fromFile("/Users/qianpeng/Desktop/app.log")
		val liens = source.getLines().toList
		source.close()
		println(liens.map ( x => if (x.indexOf("***") > -1) 1 else 0).sum)
	}
	
	test("read py save") {
		val source = Source.fromFile("/Users/qianpeng/Desktop/aaa.csv")
		val lines = source.getLines().toList
		source.close()
		
		val spark = BPSparkSession()
		lines.foreach { line =>
			val tmp = line.split(",")
			val id = tmp(0)
			val length = tmp(1)
			val hdfsUrl = tmp(2)
			
			val reading = spark.read.csv(hdfsUrl)
			val count = reading.count()
			println(s"$count <====> $length -----> ${count == length.toLong}")
//			if (length.toLong != count) {
//				println(s"$id,$hdfsUrl")
//			}
		
		}
	}
	
	test("read parquet count") {
		val source = Source.fromFile("/Users/qianpeng/Desktop/Untitled.csv")
		val lines = source.getLines().toList
		source.close()
		
		val spark = BPSparkSession()
		lines.foreach { line =>
			val tmp = line.split(",")
			val id = tmp(0)
			val length = tmp(1)
			val hdfsUrl = tmp(2)
			
			val reading = spark.read.parquet(hdfsUrl)
			val count = reading.count()
			if (length.toLong != count) {
				println(s"$id,$hdfsUrl")
			} else {
				println(s"$count")
			}
			
		}
	}

	test("replace") {
		val source = Source.fromFile("/Users/qianpeng/Desktop/差的数据")
		val lines = source.getLines().toList
		source.close()
		
		lines.foreach { line =>
			println(line.replaceAll("""\\""", """\\\\"""))
		}
		
	}
}
