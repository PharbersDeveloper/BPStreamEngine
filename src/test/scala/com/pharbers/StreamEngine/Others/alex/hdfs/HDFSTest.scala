package com.pharbers.StreamEngine.Others.alex.hdfs

import com.pharbers.StreamEngine.Utils.Strategy.hdfs.BPSHDFSFile
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import org.scalatest.FunSuite

import scala.io.Source

class HDFSTest extends FunSuite {
	test("HDFS Append Test") {
		try {
			1 to 100 foreach { x =>
				BPSHDFSFile.appendLine2HDFS(s"/jobs/test/$x", "Fuck")
			}
		} catch {
			case e: Exception => e.printStackTrace()
		}
	}

	test("Read Parquet Test") {
		val spark = BPSparkSession(null)
		val hdfsUrl = "/jobs/16574115-67b0-4c0a-8aea-8121abc8b3a0/35a7202c-1358-426c-b4fb-4ae4914c5153/contents"
		val reading = spark.read.parquet(hdfsUrl)
		reading.show()
		val count = reading.count()
		println(count)
	}
}
