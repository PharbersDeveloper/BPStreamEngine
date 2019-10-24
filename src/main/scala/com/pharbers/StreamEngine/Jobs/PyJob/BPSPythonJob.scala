package com.pharbers.StreamEngine.Jobs.PyJob

import org.apache.spark.sql
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
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

    override def open(): Unit = {
        inputStream = is
    }

    override def exec(): Unit = {
        // call the python code to exec
        inputStream match {
            case Some(is) =>
                is.writeStream
                        .format("parquet")
                        .outputMode("append")
                        .option("checkpointLocation", "/test/qi/" + this.id + "/checkpoint")
                        .option("path", "/test/qi/" + this.id + "/files")
                        .foreach(new ForeachWriter[Row](){
                            override def open(partitionId: Long, version: Long): Boolean ={
                                 true
                            }
                            override def process(value: Row): Unit ={
                                println(value)
                                Unit
                            }
                            override def close(errorOrNull: Throwable): Unit ={
                                Unit
                            }
                        })
                        .start()
                        .awaitTermination()
            case None => ???
        }
        inputStream.get.show(false)

    }

    override def close(): Unit = {
        super.close()
        container.finishJobWithId(id)
    }
}
