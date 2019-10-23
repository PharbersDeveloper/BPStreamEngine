package com.pharbers.StreamEngine.Jobs.OssJob

import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSJobStrategy
import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}
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
    type T = BPSJobStrategy
    override val strategy = null

    override def exec(): Unit = {
        inputStream = is
    }

    override def close(): Unit = {
        super.close()
        container.finishJobWithId(id)
    }
}
