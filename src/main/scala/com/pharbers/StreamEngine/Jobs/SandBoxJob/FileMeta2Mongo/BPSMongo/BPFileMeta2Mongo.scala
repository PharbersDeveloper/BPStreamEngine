package com.pharbers.StreamEngine.Jobs.SandBoxJob.FileMeta2Mongo.BPSMongo

import com.mongodb.casbah.Imports._

object BPFileMeta2Mongo {
	def apply(traceId: String,
	          sampleData: List[String],
	          schema: String,
	          length: Int): BPFileMeta2Mongo =
		new BPFileMeta2Mongo(traceId, sampleData, schema, length)
}

class BPFileMeta2Mongo(traceId: String,
                       sampleData: List[String],
                       schema: String,
                       length: Int) extends MongoTrait {
	def MetaData(): Unit ={
		queryObject(DBObject("trace-id" -> traceId), "FileMetaDatum") match {
			case None =>
			case Some(dbo) =>
				dbo += "sample-data" -> sampleData
				dbo += "schema" -> schema
				dbo += "length" -> length
				updateObject(DBObject("trace-id" -> traceId), "FileMetaDatum", dbo)
		}
	}
}
