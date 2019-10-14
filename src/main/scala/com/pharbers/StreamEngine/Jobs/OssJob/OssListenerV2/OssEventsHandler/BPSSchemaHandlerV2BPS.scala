package com.pharbers.StreamEngine.Jobs.OssJob.OssListenerV2.OssEventsHandler

import com.pharbers.StreamEngine.Utils.StreamJob.BPStreamJob
import com.pharbers.StreamEngine.Jobs.OssJob.OssListenerV2.BPSOssEndListenerV2
import com.pharbers.StreamEngine.Utils.Event.EventHandler.BPSEventHandler
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types._
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/10/11 13:40
  * @note 一些值得注意的地方
  */
case class BPSSchemaHandlerV2BPS(schemaEvent: BPSEvents) extends BPSEventHandler {
    override def exec(job: BPStreamJob)(e: BPSEvents): Unit = {
        // 收到Schema 开始执行分流
        val jobId = event2JobId(schemaEvent)
        val spark = job.spark
        import spark.implicits._
        job.inputStream match {
            case Some(input) => {
                val query = input.filter($"type" === "SandBox" && $"jobId" === jobId)
                        .select(
                            from_json($"data", event2SqlType(schemaEvent)).as("data")
                        ).select("data.*")
                        .writeStream
                        .outputMode("append")
                        .format("parquet")
                        .option("checkpointLocation", "/test/streaming/" + jobId + "/checkpoint")
                        .option("path", "/test/streaming/" + jobId + "/files")
                        .start()
                job.outputStream = query :: job.outputStream
                val endListenerV2 = new BPSOssEndListenerV2(spark, job, jobId, e.timestamp, query)
                endListenerV2.active(null)
                job.listeners = endListenerV2 :: job.listeners
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

    override def close(): Unit = {}
}
case class BPSchemaParseElement(key: String, `type`: String)
