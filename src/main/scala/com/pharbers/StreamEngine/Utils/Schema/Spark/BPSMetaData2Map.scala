package com.pharbers.StreamEngine.Utils.Schema.Spark

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read

/** 功能描述
 *
 * @param args 构造参数
 * @tparam T 构造泛型参数
 * @author clock
 * @version 0.0
 * @since 2019/11/21 18:18
 * @note 一些值得注意的地方
 */
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
