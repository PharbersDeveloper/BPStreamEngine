package com.pharbers.StreamEngine.Jobs.PyJob

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSJobStrategy
import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}

object BPSPythonJob {
    def apply(id: String,
              spark: SparkSession,
              inputStream: Option[sql.DataFrame],
              container: BPSJobContainer): BPSPythonJob =
        new BPSPythonJob(id, spark, inputStream, container)
}

class BPSPythonJob(val id: String,
                   val spark: SparkSession,
                   val is: Option[sql.DataFrame],
                   val container: BPSJobContainer) extends BPStreamJob {

    type T = BPSJobStrategy
    override val strategy: BPSJobStrategy = null

    override def exec(): Unit = {
        // call the python code to exec
        inputStream = is
//        inputStream match {
//            case Some(is) =>
//                is.foreach()
//            case None => ???
//        }
        inputStream.get.show(false)

    }

    override def close(): Unit = {
        super.close()
        container.finishJobWithId(id)
    }
}
