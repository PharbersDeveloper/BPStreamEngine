package com.pharbers.StreamEngine.BPStreamJob.FileJobs

import com.pharbers.StreamEngine.BPStreamJob.BPSJobContainer.BPSJobContainer
import com.pharbers.StreamEngine.BPStreamJob.BPStreamJob
import com.pharbers.StreamEngine.BPStreamJob.JobStrategy.JobStrategy
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

import scala.None

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
    type T = JobStrategy
    override val strategy = null

    override def exec(): Unit = {
        inputStream = is
    }

    override def close(): Unit = {
        super.close()
        container.finishJobWithId(id)
    }

}
