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
object SchemaConverter {
	
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
	
	// 废弃
	def renameColumn(jsonStr: String): String =  {
		implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)
		
		var tmpNum = 0
		val schemas = parse(jsonStr).extract[List[BPSchemaParseElement]]
		val schemaKeys = schemas.map(_.key)
		val schemaKeysDis = schemaKeys.distinct
		val diffSchemaKeys = schemaKeys diff schemaKeysDis
		val renameSchema = schemas.map{ x =>
			if (diffSchemaKeys.contains(x.key)) {
				tmpNum += 1
				BPSchemaParseElement(s"${x.key}_$tmpNum", x.`type`)
			} else {
				BPSchemaParseElement(x.key, x.`type`)
			}
		}
		val toJsonStr = write(renameSchema)
		toJsonStr
	}
	
	def column2legal(colu: String, df: DataFrame): DataFrame = {
		df.withColumn(colu, regexp_replace(col(colu), """\\"""", ""))
			.withColumn(colu, regexp_replace(col(colu) , " ", ""))
//			.withColumn(colu, regexp_replace(col(colu) , ",", ""))
//			.withColumn(colu, regexp_replace(col(colu) , ";", ""))
//			.withColumn(colu, regexp_replace(col(colu) , "\\{", ""))
//			.withColumn(colu, regexp_replace(col(colu) , "\\}", ""))
			.withColumn(colu, regexp_replace(col(colu) , "\\(", ""))
			.withColumn(colu, regexp_replace(col(colu) , "\\)", ""))
			.withColumn(colu, regexp_replace(col(colu) , "=", ""))
    		.withColumn(colu, regexp_replace(col(colu) , "\\\\n|\\\\t", ""))
	}
}

