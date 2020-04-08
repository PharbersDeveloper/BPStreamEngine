package com.pharbers.StreamEngine.Jobs.OssJob

import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Job.{BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Utils.Strategy.BPStrategyComponent
import org.apache.kafka.common.config.ConfigDef
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

object BPSOssJob {
    def apply(
                 id: String,
                 spark: SparkSession,
                 inputStream: Option[sql.DataFrame],
                 container: BPSJobContainer): BPSOssJob =
        new BPSOssJob(id, spark, inputStream, container)
}

class BPSOssJob(
                   val id: String,
                   val spark: SparkSession,
                   val is: Option[sql.DataFrame],
                   val container: BPSJobContainer) extends BPStreamJob {
    type T = BPStrategyComponent
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

    override val description: String = "oss_job"
}
