package com.pharbers.StreamEngine.BPStreamJob.FileJobs.OssListener.OssEventsHandler

import com.pharbers.StreamEngine.BPStreamJob.BPStreamJob
import com.pharbers.StreamEngine.BPStreamJob.FileJobs.OssListener.BPSOssEndListener
import com.pharbers.StreamEngine.Common.EventHandler.EventHandler
import com.pharbers.StreamEngine.Common.Events
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read

case class BPSEndLengthHandler() extends EventHandler {
    override def exec(job: BPStreamJob)(e: Events): Unit = {
        // 收到Schema 开始执行分流
        val spark = job.spark
        import spark.implicits._
        job.inputStream match {
            case Some(input) => {
                job.outputStream = input.filter($"type" === "SandBox" && $"jobId" == event2JobId(e))
                    .writeStream
                    .outputMode("complete")
                    .format("memory")
                    .queryName(event2JobId(e))
                    .start() :: job.outputStream

                new BPSOssEndListener(spark, job, event2JobId(e), event2Length(e)).active(null)
            }
            case None => ???
        }
    }

    def event2JobId(e: Events): String = e.jobId
    def event2Length(e: Events): Int = {
        implicit val formats = DefaultFormats
        read[BPEndLengthElement](e.data).length
    }
}

case class BPEndLengthElement(length: Int)