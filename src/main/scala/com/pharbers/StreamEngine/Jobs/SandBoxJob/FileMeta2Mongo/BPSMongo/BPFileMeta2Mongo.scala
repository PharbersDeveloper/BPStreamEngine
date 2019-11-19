package com.pharbers.StreamEngine.Jobs.SandBoxJob.FileMeta2Mongo.BPSMongo

import com.mongodb.casbah.Imports._
import com.mongodb.casbah.query.Imports
import com.mongodb.casbah.query.dsl.QueryExpressionObject

object BPFileMeta2Mongo {
	def apply(jobId: String,
	          schema: Map[String, String]): BPFileMeta2Mongo =
		new BPFileMeta2Mongo(jobId, schema)
}

class BPFileMeta2Mongo(jobId: String,
                       schema: Map[String, String]) extends MongoTrait {
	
	def convertSchema2Mongo(): Unit = {
		val condition: DBObject = DBObject("jobId" -> jobId)
		queryObject(condition, "datasets") match {
			case None =>
			case Some(dbo) =>
				println(dbo)
//				dbo += "schema" -> schema
//				updateObject(condition, "datasets", dbo)
		}
	}
}
