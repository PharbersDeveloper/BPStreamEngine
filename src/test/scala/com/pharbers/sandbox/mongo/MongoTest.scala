package com.pharbers.sandbox.mongo

import com.pharbers.StreamEngine.Jobs.SandBoxJob.FileMeta2Mongo.BPSMongo.MongoTrait
import org.scalatest.FunSuite
import com.mongodb.casbah.Imports._

class MongoTest extends FunSuite with MongoTrait{
	test("Mongo test") {
		val condition = ("file-version-ids" $in "5d329a36421aa93290f1c920" :: Nil)
		println(condition.toString)
		queryObject(condition, "FileMetaDatum") match {
			case None => println("None")
			case Some(res) =>
				println(res)
//				res += "name" -> "测评文案000.xlsx" //5d3299f4421aa93290f1c919
//				res.getAs[List[String]]("file-version-ids").map(r => r ++ "bb" ++ "cc")
//				res += "file-version-ids" -> res.getAs[List[String]]("file-version-ids").map(r => r :+ "bb" :+ "cc")
//				updateObject(DBObject("group-id" -> "1"), "FileMetaDatum", res)
		}
		
	}
}
