package com.pharbers.StreamEngine.Jobs.OssPartitionJob

import com.pharbers.StreamEngine.Utils.Channel.Worker.BPSWorkerChannel
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Job.{BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Utils.Strategy.JobStrategy.BPSCommonJobStrategy
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
    type T = BPSCommonJobStrategy
    override val strategy = BPSCommonJobStrategy(componentProperty, configDef)
    override val id: String = strategy.getId
    val jobId: String = strategy.getJobId
    val spark: SparkSession = strategy.getSpark

    override def open(): Unit = {
        inputStream = container.inputStream
    }

    override def exec(): Unit = inputStream match {
        case Some(is) => {
            outputStream = outputStream :+ startDataJob(is)
        }
        case None => ???
    }

    override def close(): Unit = {
        super.close()
        container.finishJobWithId(id)
    }

    def startDataJob(df: DataFrame): StreamingQuery = {
        df.filter(col("type") === "SandBox").writeStream
                .partitionBy("jobId")
                .format("parquet")
                .outputMode("append")
                .option("checkpointLocation", getCheckpointPath)
                .option("path", getOutputPath)
                .start()
    }

    override def createConfigDef(): ConfigDef =  new ConfigDef()

    override val description: String = "BPSOssPartitionJob"
}
