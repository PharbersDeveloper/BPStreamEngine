package com.pharbers.StreamEngine.Jobs.SandBoxJob.FileMeta2Mongo.BPSMongo

import com.mongodb.casbah.Imports._

import scala.xml.{Elem, XML}

sealed trait MongoInstance {
	lazy val mongoXml: Elem = XML.load("src/main/resources/sandbox_mongo_connect.xml")
	lazy val url: String = (mongoXml \ "server_host" \ "@value").toString()
	lazy val port: Int = (mongoXml \ "server_port" \ "@value").toString().toInt
	lazy val databaseName: String = (mongoXml \ "conn_name" \ "@value").toString()
	lazy val dbIns = MongoClient(url, port)
	
}

trait MongoTrait extends MongoInstance {
	import collection.JavaConverters._
	def queryObject(condition: DBObject, coll: String): Option[DBObject] = {
		val collect = dbIns.getDB(databaseName).getCollection(coll)
		val result = collect.find(condition).toArray.asScala.toList match {
			case Nil => None
			case res => Some(res.head)
		}
		result
	}
	
	def updateObject(condition: DBObject, coll: String, obj: DBObject): Int = {
		val collect = dbIns.getDB(databaseName).getCollection(coll)
		val result = collect.update(condition, obj).getN
		result
	}
}
