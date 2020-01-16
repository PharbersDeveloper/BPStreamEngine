package com.pharbers.StreamEngine.Others.alex.hdfs

import com.pharbers.StreamEngine.Utils.HDFS.BPSHDFSFile
import com.pharbers.StreamEngine.Utils.Session.Spark.BPSparkSession
import org.scalatest.FunSuite

import scala.io.Source

class HDFSTest extends FunSuite {
	test("HDFS Test") {
		try {
			1 to 100 foreach { x =>
				BPSHDFSFile.appendLine2HDFS(s"/jobs/test/$x", "Fuck")
			}
		} catch {
			case e: Exception => e.printStackTrace()
		}
		
	}
}
