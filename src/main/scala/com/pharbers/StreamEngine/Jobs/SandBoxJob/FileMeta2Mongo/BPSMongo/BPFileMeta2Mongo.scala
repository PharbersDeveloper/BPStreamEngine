package com.pharbers.StreamEngine.Jobs.SandBoxJob.FileMeta2Mongo.BPSMongo

object BPFileMeta2Mongo {
	def apply(traceId: String, line: String): BPFileMeta2Mongo = new BPFileMeta2Mongo(traceId, line)
}

class BPFileMeta2Mongo(traceId: String,
                       line: String) extends MongoTrait {
	def MetaData(): Unit ={
	
	}
}
