package com.pharbers.StreamEngine.OssJobs.FileJobs.OssListener.OssEventsHandler

import com.pharbers.StreamEngine.BPStreamJob.BPStreamJob
import com.pharbers.StreamEngine.Common.Event.EventHandler.BPSEventHandler
import com.pharbers.StreamEngine.Common.Event.BPSEvents
import org.apache.spark.sql.types._
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read
import org.apache.spark.sql.functions._

case class BPSSchemaHandlerBPS() extends BPSEventHandler {
    override def exec(job: BPStreamJob)(e: BPSEvents): Unit = {
        // 收到Schema 开始执行分流
        val jobId = event2JobId(e)
        val spark = job.spark
        import spark.implicits._
        job.inputStream match {
            case Some(input) => {
                job.outputStream = input.filter($"type" === "SandBox" && $"jobId" === jobId)
                    .select(
                        from_json($"data", event2SqlType(e)).as("data")
                    ).select("data.*")
                    .writeStream
                    .outputMode("append")
                    .format("csv")
                    .option("checkpointLocation", "/test/streaming/" + jobId + "/checkpoint")
                    .option("path", "/test/streaming/" + jobId + "/files")
                    .start() :: job.outputStream
            }

            case None => ???
        }
    }

    def event2JobId(e: BPSEvents): String = e.jobId
    def event2SqlType(e: BPSEvents): org.apache.spark.sql.types.DataType = {
        // TODO: 以后全变为AVRO的Schema形式
//        SchemaConverters.toSqlType(new Schema.Parser().parse(e.data)).dataType
        implicit val formats = DefaultFormats
        val lst = read[List[BPSchemaParseElement]](e.data)
        StructType(
            lst.map( x => StructField(x.key, x.`type` match {
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

    override def close(): Unit = {}
}

case class BPSchemaParseElement(key: String, `type`: String)
