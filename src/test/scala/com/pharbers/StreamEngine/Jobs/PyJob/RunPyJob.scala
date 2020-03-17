package com.pharbers.StreamEngine.Jobs.PyJob

import java.util.UUID

import org.json4s._
import java.util.concurrent.TimeUnit

import com.pharbers.kafka.schema.BPJob
import org.json4s.jackson.Serialization.write
import com.pharbers.kafka.producer.PharbersKafkaProducer
import com.pharbers.StreamEngine.Utils.Component.Dynamic.JobMsg
import com.pharbers.StreamEngine.Utils.Session.Spark.BPSparkSession
import com.pharbers.StreamEngine.Jobs.PyJob.PythonJobContainer.BPSPythonJobContainer

object RunPyJob extends App {

    def directStart(): Unit = {
        val job = BPSPythonJobContainer(null, BPSparkSession(), Map.empty)
        job.open()
        job.exec()
    }
//    directStart()

    def sendMsgStart(): Unit = {
        implicit val formats: DefaultFormats.type = DefaultFormats

        val jobId = "201910231514"
        val traceId = "201910231514"
        val `type` = "addList"
        val clazz = "com.pharbers.StreamEngine.Jobs.PyJob.PythonJobContainer.BPSPythonJobContainer"
//        val jobs = JobMsg("pyBoxJob", "job", clazz, List("$BPSparkSession"), Nil, Nil, Map(
//            "jobId" -> UUID.randomUUID().toString,
//            "metadataPath" -> "hdfs:///jobs/1d157501-3b43-486a-88e6-fbfbe02a0c84/5344a3fe-2b18-448f-a82f-04d917e05ad1/metadata",
//            "filesPath" -> "hdfs:///jobs/1d157501-3b43-486a-88e6-fbfbe02a0c84/5344a3fe-2b18-448f-a82f-04d917e05ad1/contents/5344a3fe-2b18-448f-a82f-04d917e05ad1",
//            "resultPath" -> "./jobs/"
//        ), "", "py job") :: Nil
        val jobs = JobMsg("pyBoxJob", "job", clazz, List("$BPSparkSession"), Nil, Nil, Map(
            "jobId" -> UUID.randomUUID().toString,
            "metadataPath" -> "/jobs/16574115-67b0-4c0a-8aea-8121abc8b3a0/35a7202c-1358-426c-b4fb-4ae4914c5153/metadata",
            "filesPath" -> "/jobs/16574115-67b0-4c0a-8aea-8121abc8b3a0/35a7202c-1358-426c-b4fb-4ae4914c5153/contents",
            "resultPath" -> "./jobs/"
        ), "", "py job") :: Nil

        val jobMsg = write(jobs)
        val topic = "stream_job_submit_qi"

        val pkp = new PharbersKafkaProducer[String, BPJob]
        val bpJob = new BPJob(jobId, traceId, `type`, jobMsg)
        val fu = pkp.produce(topic, jobId, bpJob)
        println(fu.get(10, TimeUnit.SECONDS))
    }

    sendMsgStart()

}
