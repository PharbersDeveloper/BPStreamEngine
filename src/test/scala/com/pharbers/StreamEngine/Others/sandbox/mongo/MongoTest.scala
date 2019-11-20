package com.pharbers.StreamEngine.Others.sandbox.mongo

import com.pharbers.StreamEngine.Jobs.SandBoxJob.FileMeta2Mongo.BPSMongo.{BPFileMeta2Mongo, MongoTrait}
import org.scalatest.FunSuite
import com.mongodb.casbah.Imports._

class MongoTest extends FunSuite with MongoTrait {
	test("Mongo test") {
//		32952-5e1d-4931-b5df-bbdab
		
		val condition = DBObject("jobId" -> "db7c8-b275-4d72-adc3-aa4070")
		queryObject(condition, "datasets") match {
			case None =>
			case Some(obj) => {
			
			}
		}
		
//
//		1 to 1 foreach { _ =>
//			BPFileMeta2Mongo("", Nil, "", 0).SampleData()
//			queryObject(condition, "FileMetaDatum") match {
//				case None => println("None")
//				case Some(res) =>
//					println(res)
//				//				res += "name" -> "测评文案000.xlsx" //5d3299f4421aa93290f1c919
//				//				res.getAs[List[String]]("file-version-ids").map(r => r ++ "bb" ++ "cc")
//				//				res += "file-version-ids" -> res.getAs[List[String]]("file-version-ids").map(r => r :+ "bb" :+ "cc")
//				//				updateObject(DBObject("group-id" -> "1"), "FileMetaDatum", res)
//			}
//		}
//
	}
}
