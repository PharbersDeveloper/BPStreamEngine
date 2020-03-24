package com.pharbers.StreamEngine.Utils.Strategy.Schema

import com.pharbers.StreamEngine.Utils.Annotation.Component
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Strategy.BPStrategyComponent
import org.apache.kafka.common.config.ConfigDef
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

@Component(name = "BPSMetaData2Map", `type` = "BPSMetaData2Map")
case class BPSMetaData2Map(override val componentProperty: Component2.BPComponentConfig)
	extends BPStrategyComponent {

	def list2Map(jsonList: List[String]): Map[String, AnyRef] = {
		implicit val formats: DefaultFormats.type = DefaultFormats
		var jsonMap: Map[String, AnyRef] = Map()
		jsonList.foreach{ line =>
			try {
				val schema = read[List[Map[String, AnyRef]]](line)
				jsonMap = jsonMap ++ Map("schema" -> schema)
			} catch {
				case _: Throwable =>
					val obj = read[Map[String, AnyRef]](line)
					obj match {
						case o if o.contains("label") => jsonMap = jsonMap ++ Map("tag" -> o)
						case o if o.contains("length") => jsonMap = jsonMap ++ Map("length" -> o("length"))
						case _ =>
					}
			}
		}
		jsonMap
	}

	def str2Map(jsonObjectStr: String): Map[String, AnyRef] = ???

	override val strategyName: String = "mate2map"
	override def createConfigDef(): ConfigDef = ???
}
