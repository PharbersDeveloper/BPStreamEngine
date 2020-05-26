package com.pharbers.StreamEngine.Jobs.OssPartitionJob

import java.sql.Timestamp

import com.pharbers.StreamEngine.Utils.Channel.Worker.BPSWorkerChannel
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Job.{BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Utils.Strategy.JobStrategy.BPSCommonJobStrategy
import org.apache.kafka.common.config.ConfigDef
import org.apache.spark.TaskContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/04/08 18:37
  * @note 一些值得注意的地方
  */
class BPSKafkaMsgJob(container: BPSJobContainer, val componentProperty: Component2.BPComponentConfig) extends BPStreamJob {
    override type T = BPSCommonJobStrategy
    override val strategy: BPSCommonJobStrategy = BPSCommonJobStrategy(componentProperty, configDef)
    override val id: String = componentProperty.id
    override val description: String = "kafka_msg_job"
    override val spark: SparkSession = strategy.getSpark

    override def createConfigDef(): ConfigDef = new ConfigDef

    override def open(): Unit = {
        inputStream = container.inputStream
    }

    override def exec(): Unit = inputStream match {
        case Some(is) => {
            outputStream = outputStream :+ startMsgJob(is)
        }
        case None => ???
    }

    def startMsgJob(df: DataFrame): StreamingQuery = {
        df.filter(col("type") =!= "SandBox").writeStream
                .foreach(
                    new ForeachWriter[Row] {

                        var channel: Option[BPSWorkerChannel] = None

                        def open(partitionId: Long, version: Long): Boolean = {
                            if (channel.isEmpty) channel = Some(BPSWorkerChannel(TaskContext.get().getLocalProperty("host")))
                            true
                        }

                        def process(value: Row): Unit = {

                            implicit val formats: DefaultFormats.type = DefaultFormats
                            val `type` =  value.getAs[String]("type")
                            val data = if(`type` == "SandBox-Schema" || `type` == "SandBox-Labels" || `type` == "SandBox-Length") {
                                Map("data" -> value.getAs[String]("data"), "id" -> value.getAs[String]("id"))
                            } else value.getAs[String]("data")

                            val event = BPSEvents(
                                value.getAs[String]("jobId"),
                                value.getAs[String]("traceId"),
                                value.getAs[String]("type"),
                                data,
                                value.getAs[Timestamp]("timestamp")
                            )

                            channel.get.pushMessage(write(event))
                        }

                        def close(errorOrNull: scala.Throwable): Unit = {
                            channel.get.close()
                        }
                    }
                )
                .option("checkpointLocation", getCheckpointPath)
                .start()
    }

    override def close(): Unit = {
        super.close()
        container.finishJobWithId(id)
    }
}
