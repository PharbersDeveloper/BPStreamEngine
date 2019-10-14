package com.pharbers.StreamEngine.PyJobs

import com.pharbers.StreamEngine.BPStreamJob.BPStreamJob
import com.pharbers.StreamEngine.BPStreamJob.JobStrategy.JobStrategy
import com.pharbers.StreamEngine.OssJobs.BPSJobContainer.BPSJobContainer
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

class BPSPythonJob(
                  val id: String,
                  val spark: SparkSession,
                  val is: Option[sql.DataFrame],
                  val container: BPSJobContainer) extends BPStreamJob {

    type T = JobStrategy
    override val strategy: JobStrategy = null

    override def exec(): Unit = {
        // TODO: call the python code to exec
    }

    override def close(): Unit = {
        super.close()
        container.finishJobWithId(id)
    }
}
