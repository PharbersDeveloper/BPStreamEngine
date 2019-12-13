package com.pharbers.StreamEngine.Jobs.SandBoxJob

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BinaryType, BooleanType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType}
import org.json4s._
import org.json4s.jackson.Serialization._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

case class BPSchemaParseElement(key: String, `type`: String)

// TODO 只是简单放置，需要抽象
object SchemaConverter extends Serializable {
	def str2SqlType(data: String): org.apache.spark.sql.types.DataType = {
		implicit val formats: DefaultFormats.type = DefaultFormats
		val lstData: List[BPSchemaParseElement] = read[List[BPSchemaParseElement]](data)
		StructType(
			lstData.map(x => StructField(x.key, x.`type` match {
				case "String" => StringType
				case "Int" => IntegerType
				case "Boolean" => BooleanType
				case "Byte" => BinaryType
				case "Double" => DoubleType
				case "Float" => FloatType
				case "Long" => LongType
				case "Fixed" => BinaryType
				case "Enum" => StringType
			}))
		)
	}
	
	def column2legalWithDF(colu: String, df: DataFrame): DataFrame = {
		df.withColumn(colu, regexp_replace(col(colu), """\\"""", ""))
			.withColumn(colu, regexp_replace(col(colu) , " ", ""))
//			.withColumn(colu, regexp_replace(col(colu) , ",", ""))
			.withColumn(colu, regexp_replace(col(colu) , ";", ""))
//			.withColumn(colu, regexp_replace(col(colu) , "\\{", ""))
//			.withColumn(colu, regexp_replace(col(colu) , "\\}", ""))
			.withColumn(colu, regexp_replace(col(colu) , "\\(", ""))
			.withColumn(colu, regexp_replace(col(colu) , "\\)", ""))
			.withColumn(colu, regexp_replace(col(colu) , "=", ""))
    		.withColumn(colu, regexp_replace(col(colu) , "\\\\n|\\\\t", ""))
	}
	
	def column2legalWithMetaDataSchema(data: Map[String, AnyRef]): Map[String, AnyRef] = {
		val schema = data("schema").asInstanceOf[List[Map[String, AnyRef]]].map { m =>
			val key = m("key").toString.replaceAll("""[," ";{}()=\\n\\t\\"]""", "")
			val `type` = m("type")
			Map("key" -> key, "type" -> `type`)
		}
		Map("schema" -> schema)
	}
}

