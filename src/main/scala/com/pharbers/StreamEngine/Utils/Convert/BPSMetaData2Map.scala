package com.pharbers.StreamEngine.Utils.Convert

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read

object BPSMetaData2Map {
	def list2Map(jsonList: List[String]): Map[String, Any] = {
		implicit val formats: DefaultFormats.type = DefaultFormats
		var jsonMap: Map[String, Any] = Map()
		jsonList.foreach{ line =>
			try {
				val schema = read[List[Map[String, Any]]](line)
				jsonMap = jsonMap ++ Map("schema" -> schema)
			} catch {
				case _: Throwable =>
					val obj = read[Map[String, Any]](line)
					obj match {
						case o if o.contains("label") => jsonMap = jsonMap ++ Map("tag" -> o)
						case o if o.contains("length") => jsonMap = jsonMap ++ Map("length" -> o("length"))
						case _ =>
					}
			}
		}
		jsonMap
	}
	
	def str2Map(jsonObjectStr: String): Map[String, Any] = ???
	
}
