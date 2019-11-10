package com.pharbers.StreamEngine.Jobs.SandBoxJob

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BinaryType, BooleanType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read

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
	
	// TODO 转换不合法的Schema列名，这边抽象和使用的不对，处理不了流的，要改进
	def column2legal(colu: String, df: DataFrame): DataFrame = {
//		implicit spark: SparkSession
//		import spark.implicits._
		df.withColumn(colu, regexp_replace(col(colu), """\\"""", ""))
			.withColumn(colu, regexp_replace(col(colu) , " ", "_"))
//			.withColumn(colu, regexp_replace(col(colu) , ",", ""))
//			.withColumn(colu, regexp_replace(col(colu) , ";", ""))
//			.withColumn(colu, regexp_replace(col(colu) , "\\{", ""))
//			.withColumn(colu, regexp_replace(col(colu) , "\\}", ""))
			.withColumn(colu, regexp_replace(col(colu) , "\\(", ""))
			.withColumn(colu, regexp_replace(col(colu) , "\\)", ""))
			.withColumn(colu, regexp_replace(col(colu) , "=", ""))
    		.withColumn(colu, regexp_replace(col(colu) , "\\\\n|\\\\\t", ""))
	}
}

