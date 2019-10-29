package com.pharbers.StreamEngine.Jobs.SandBoxJob.FileMeta2Mongo.BPSMongo

import com.mongodb.casbah.Imports._
import com.mongodb.casbah.query.Imports
import com.mongodb.casbah.query.dsl.QueryExpressionObject

object BPFileMeta2Mongo {
	def apply(jobId: String,
	          sampleData: List[String],
	          schema: String,
	          length: Int): BPFileMeta2Mongo =
		new BPFileMeta2Mongo(jobId, sampleData, schema, length)
}

class BPFileMeta2Mongo(jobId: String,
                       sampleData: List[String],
                       schema: String,
                       length: Int) extends MongoTrait {
	
	val condition: DBObject = ("job-id" $in jobId :: Nil)
	
	def SampleData(): Unit = {
		queryObject(condition, "FileMetaDatum") match {
			case None =>
			case Some(dbo) =>
				dbo += "sample-data" -> sampleData
				updateObject(condition, "FileMetaDatum", dbo)
		}
	}
	
	def SchemaData(): Unit = {
		queryObject(condition, "FileMetaDatum") match {
			case None =>
			case Some(dbo) =>
				dbo += "schema" -> schema
				dbo += "length" -> length.asInstanceOf[Number]
				updateObject(condition, "FileMetaDatum", dbo)
		}
	}
}
