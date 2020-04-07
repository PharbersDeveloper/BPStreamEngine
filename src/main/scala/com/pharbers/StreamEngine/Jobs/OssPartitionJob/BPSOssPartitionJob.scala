package com.pharbers.StreamEngine.Jobs.OssPartitionJob

import com.pharbers.StreamEngine.Jobs.OssPartitionJob.OssListener.BPSOssListener
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Job.{BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Utils.Strategy.BPStrategyComponent
import org.apache.kafka.common.config.ConfigDef
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

object BPSOssPartitionJob {
    def apply(id: String,
              spark: SparkSession,
              inputStream: Option[sql.DataFrame],
              container: BPSJobContainer): BPSOssPartitionJob =
        new BPSOssPartitionJob(id, spark, inputStream, container)
}

class BPSOssPartitionJob(
                   val id: String,
                   val spark: SparkSession,
                   val is: Option[sql.DataFrame],
                   val container: BPSJobContainer) extends BPStreamJob {
    type T = BPStrategyComponent
    override val strategy = null

    override def open(): Unit = {
        inputStream = is
    }

    override def exec(): Unit = {
        case Some(is) => {
            val listener = BPSOssListener(spark, this, jobId)
            listener.active(is)

            listeners = listener :: listeners

            is.filter($"type" === "SandBox").writeStream
                    .partitionBy("jobId")
                    .format("parquet")
                    .outputMode("append")
                    .option("checkpointLocation", getCheckpointPath)
                    .option("path", getOutputPath)
                    .start()
        }
        case None => ???
    }

    override def close(): Unit = {
        super.close()
        container.finishJobWithId(id)
    }

    override val componentProperty: Component2.BPComponentConfig = null

    override def createConfigDef(): ConfigDef = ???

    override val description: String = "InputStream"
}
