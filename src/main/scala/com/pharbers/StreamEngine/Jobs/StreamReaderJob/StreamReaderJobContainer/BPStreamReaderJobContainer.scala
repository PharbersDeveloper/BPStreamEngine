package com.pharbers.StreamEngine.Jobs.StreamReaderJob.StreamReaderJobContainer

import java.sql.Timestamp
import java.util.UUID

import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.StreamJob.BPSJobContainer
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy
import org.apache.kafka.common.config.ConfigDef
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object BPStreamReaderJobContainer {
    def apply(spark: SparkSession): BPStreamReaderJobContainer = new BPStreamReaderJobContainer(spark)
}

class BPStreamReaderJobContainer(override val spark: SparkSession) extends BPSJobContainer {
    val id = UUID.randomUUID().toString
    type T = BPSKfkJobStrategy
    override val strategy = null

    import spark.implicits._
    override def open(): Unit = {
        // TODO: Read Stream, 这个地方应该是向下抽象，我不想写，向下抽象看能不能留给你们

        val loadSchema =
            StructType(
                StructField("jobId", StringType) ::
                StructField("traceId", StringType) ::
                StructField("type", StringType) ::
                StructField("data", StringType) ::
                StructField("timestamp", TimestampType) :: Nil
            )
//        (jobId: String, traceId: String, `type`: String, data: String, timestamp: Timestamp = new Timestamp(0))

        this.inputStream = Some(spark.readStream
//            .format("parquet")
            .schema(loadSchema)
            .parquet("/test/streamingV2/0829b025-48ac-450c-843c-6d4ee91765ca/files/jobId=01e25-e9b3-4efe-aec6-0bece0"))
    }

    override def exec(): Unit = inputStream match {
        case Some(is) => {
            // TODO: 应该不在这里， 只是一个Demo
            is.writeStream
                .format("console")
                .outputMode("append")
                .start()
        }
        case None => ???
    }

    override val componentProperty: Component2.BPComponentConfig = null

    override def createConfigDef(): ConfigDef = ???
}
