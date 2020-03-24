package com.pharbers.StreamEngine.Jobs.OssPartitionJob

import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Job.{BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Utils.Strategy.BPSJobStrategy
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
    type T = BPSJobStrategy
    override val strategy = null

    override def exec(): Unit = {
        inputStream = is
    }

    override def close(): Unit = {
        super.close()
        container.finishJobWithId(id)
    }

    override val componentProperty: Component2.BPComponentConfig = null

    override def createConfigDef(): ConfigDef = ???
}
