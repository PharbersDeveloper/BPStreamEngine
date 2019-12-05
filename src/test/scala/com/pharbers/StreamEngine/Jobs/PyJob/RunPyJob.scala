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
            "metadataPath" -> "hdfs:///user/alex/jobs/65793c2a-bab6-45c5-a57c-1ab4a5208b56/b7790e45-de28-4cd5-82af-504df2f029a7/metadata",
            "filesPath" -> "hdfs:///user/alex/jobs/65793c2a-bab6-45c5-a57c-1ab4a5208b56/b7790e45-de28-4cd5-82af-504df2f029a7/contents/6ecff92d-2ae4-4922-b433-7fe6ec3f7457",
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
