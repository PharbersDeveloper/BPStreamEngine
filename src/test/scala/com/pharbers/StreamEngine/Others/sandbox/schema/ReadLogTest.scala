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
	
	test("read2") {
		val source = Source.fromFile("/Users/qianpeng/Desktop/Untitled.csv")
		val lines = source.getLines().toList
		source.close()
		
		val spark = BPSparkSession()
		lines.foreach { line =>
			val tmp = line.split(",")
			val id = tmp(0)
			val hdfsUrl = tmp(1)
			
			val reading = spark.read.csv(hdfsUrl)
			val count = reading.count()
//			println(count)
			if (count == 0) {
				println(s"$id,$hdfsUrl")
			}
			
		}
	}
}
