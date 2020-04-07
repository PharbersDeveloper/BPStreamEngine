package com.pharbers.StreamEngine.Jobs.OssPartitionJob

import java.util.UUID

import com.pharbers.StreamEngine.Jobs.OssPartitionJob.OssListener.BPSOssListener
import com.pharbers.StreamEngine.Utils.Channel.Worker.BPSWorkerChannel
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Job.{BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Utils.Strategy.BPStrategyComponent
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import org.apache.kafka.common.config.ConfigDef
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

object BPSOssPartitionJob {

}

case class BPSOssPartitionJob(container: BPSJobContainer, componentProperty: Component2.BPComponentConfig) extends BPStreamJob {
    type T = BPStrategyComponent
    override val strategy = null
    //todo: 这儿如果是随机生成的uuid，那就意味着非spark自动重启job时将不能使用到CheckpointPath
    override val id: String = componentProperty.id
    val jobId: String = id
    val spark: SparkSession = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession]

    override def open(): Unit = {
        inputStream = container.inputStream
    }

    override def exec(): Unit = inputStream match {
        case Some(is) => {
            val listener = BPSOssListener(this)
            listener.active(null)
            listeners = listener :: listeners
            outputStream = outputStream :+ startMsgJob(is) :+ startDataJob(is)

        }
        case None => ???
    }

    override def close(): Unit = {
        super.close()
        container.finishJobWithId(id)
    }

    def startMsgJob(df: DataFrame): StreamingQuery = {
        df.filter(col("type") === "SandBox-Schema" || col("type") === "SandBox-Labels" || col("type") === "SandBox-Length").writeStream
                .foreach(
                    new ForeachWriter[Row] {

                        var channel: Option[BPSWorkerChannel] = None

                        def open(partitionId: Long, version: Long): Boolean = {
                            if (channel.isEmpty) channel = Some(BPSWorkerChannel(TaskContext.get().getLocalProperty("host")))
                            true
                        }

                        def process(value: Row): Unit = {

                            implicit val formats: DefaultFormats.type = DefaultFormats

                            val event = BPSEvents(
                                value.getAs[String]("jobId"),
                                value.getAs[String]("traceId"),
                                value.getAs[String]("type"),
                                value.getAs[String]("data"),
                                value.getAs[java.sql.Timestamp]("timestamp")
                            )
                            channel.get.pushMessage(write(event))
                        }

                        def close(errorOrNull: scala.Throwable): Unit = {
                            channel.get.close()
                        }
                    }
                )
                .option("checkpointLocation", s"$getCheckpointPath/msgJob")
                .start()
    }

    def startDataJob(df: DataFrame): StreamingQuery = {
        df.filter(col("type") === "SandBox").writeStream
                .partitionBy("jobId")
                .format("parquet")
                .outputMode("append")
                .option("checkpointLocation", s"$getCheckpointPath/dataJob")
                .option("path", getOutputPath)
                .start()
    }

    override def createConfigDef(): ConfigDef = ???

    override val description: String = "BPSOssPartitionJob"
}
