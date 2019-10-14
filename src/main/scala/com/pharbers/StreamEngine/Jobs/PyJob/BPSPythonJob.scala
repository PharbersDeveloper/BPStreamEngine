package com.pharbers.StreamEngine.Jobs.PyJob

import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSJobStrategy
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

class BPSPythonJob(
                  val id: String,
                  val spark: SparkSession,
                  val is: Option[sql.DataFrame],
                  val container: BPSJobContainer) extends BPStreamJob {

    type T = BPSJobStrategy
    override val strategy: BPSJobStrategy = null

    override def exec(): Unit = {
        // TODO: call the python code to exec
    }

    override def close(): Unit = {
        super.close()
        container.finishJobWithId(id)
    }
}
