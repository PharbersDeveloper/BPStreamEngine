package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxConvertSchemaJob

import java.util.UUID
import java.util.concurrent.TimeUnit

import com.pharbers.StreamEngine.Jobs.OssJob.OssListenerV2.OssEventsHandler.BPSchemaParseElement
import com.pharbers.StreamEngine.Jobs.OssPartitionJob.OssPartitionMeta.BPSOssPartitionMeta
import com.pharbers.StreamEngine.Utils.StreamJob.BPSJobContainer
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy
import com.pharbers.kafka.producer.PharbersKafkaProducer
import com.pharbers.kafka.schema.FileMetaData
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{BinaryType, BooleanType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read

object BPSSandBoxConvertSchemaJob {
	def apply(id: String,
	          metaPath: String,
	          samplePath: String,
	          jobId: String,
	          spark: SparkSession): BPSSandBoxConvertSchemaJob =
		new BPSSandBoxConvertSchemaJob(id, metaPath, samplePath, jobId, spark)
}

class BPSSandBoxConvertSchemaJob(val id: String,
                                 metaPath: String,
                                 samplePath: String,
                                 jobId: String,
                                 val spark: SparkSession)
	extends BPSJobContainer  {
	
	type T = BPSKfkJobStrategy
	val strategy = null
	import spark.implicits._
	
	override def open(): Unit = {
		// TODO 要形成过滤规则参数化
		val metaDataStream = spark.sparkContext.textFile(s"$metaPath/$jobId").toDF("MetaData")
		
		val repMetaDataStream = metaDataStream.head()
			.getAs[String]("MetaData")
			.replaceAll("""\\"""", "")
			.replaceAll(" ", "_")
		
		metaDataStream.foreach{x =>
			val line = x.getAs[String]("MetaData")
				.replaceAll("""\\"""", "")
				.replaceAll(" ", "_")
			
			BPSOssPartitionMeta.pushLineToHDFS(id, jobId, line)
		}
		
		val schema = event2SqlType(repMetaDataStream)

		inputStream = Some(
			spark.readStream
				.schema(StructType(
					StructField("traceId", StringType) ::
						StructField("type", StringType) ::
						StructField("data", StringType) ::
						StructField("timestamp", TimestampType) :: Nil
				))
				.parquet(s"$samplePath$jobId")
    			.filter($"type" === "SandBox")
				.withColumn("data", regexp_replace($"data" , """\\"""", ""))
				.withColumn("data", regexp_replace($"data" , " ", "_"))
				.select(
					col("traceId"),
					from_json($"data", schema).as("data"),
					col("timestamp").as("unixTimestamp")
				)
		)
	}
	
	override def exec(): Unit = {
		inputStream match {
			case Some(is) =>
				is.writeStream
					.outputMode("append")
					.format("parquet")
//					.format("console")
					.option("checkpointLocation", "/test/alex/" + id + "/checkpoint")
					.option("path", "/test/alex/" + id + "/files")
					.start()
				
//				pollKafka(new FileMetaData(id, jobId, "/test/alex/" + id + "/metadata/" + "",
//					"/test/alex/" + id + "/files/" + "jobId=" + "", ""))
				
			case None =>
		}
	}
	
	def event2SqlType(data: String): org.apache.spark.sql.types.DataType = {
		// TODO: 以后全变为AVRO的Schema形式
		//                SchemaConverters.toSqlType(new Schema.Parser().parse(e.data)).dataType
		implicit val formats = DefaultFormats
		val lst = read[List[BPSchemaParseElement]](data)
		StructType(
			lst.map(x => StructField(x.key, x.`type` match {
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
	
	def pollKafka(msg: FileMetaData): Unit ={
		//todo: 参数化
		val topic = "sb_file_meta_job_test"
		val pkp = new PharbersKafkaProducer[String, FileMetaData]
		val fu = pkp.produce(topic, msg.getJobId.toString, msg)
		println(fu.get(10, TimeUnit.SECONDS))
	}
}

case class BPSchemaParseElement(key: String, `type`: String)
