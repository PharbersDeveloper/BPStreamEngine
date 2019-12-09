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
        val job = BPSPythonJobContainer(null, BPSparkSession(), Map(
            "containerId" -> UUID.randomUUID().toString,
            "listenerTopic" -> "PyJobContainerListenerTopic",
            "noticeTopic" -> "PyJobContainerNoticeTopic",
            "process" -> "4",
            "partition" -> "4",
            "pythonUri" -> "https://github.com/PharbersDeveloper/bp-data-clean.git",
            "pythonBranch" -> "v0.0.1"
        ))
        job.open()
        job.exec()
    }
    directStart()

    def sendMsgStart(): Unit = {
        implicit val formats: DefaultFormats.type = DefaultFormats

        val jobId = "201910231514"
        val traceId = "201910231514"
        val `type` = "addList"

        val jobMsg = write(Map(
            "jobId" -> UUID.randomUUID().toString,
            "metadataPath" -> "hdfs:///user/alex/jobs/94350fa8-420e-4f5f-8f1e-467627aafec3/ff94c3bd-1887-4800-a5f4-43a292869008/metadata",
            "filesPath" -> "hdfs:///user/alex/jobs/94350fa8-420e-4f5f-8f1e-467627aafec3/ff94c3bd-1887-4800-a5f4-43a292869008/contents/6f6b8e34-88df-4bef-a930-20197ccd6ef0",
            "resultPath" -> "./jobs/"
        ))
        val topic = "PyJobContainerListenerTopic"

        val pkp = new PharbersKafkaProducer[String, BPJob]
        val bpJob = new BPJob(jobId, traceId, `type`, jobMsg)
        val fu = pkp.produce(topic, jobId, bpJob)
        println(fu.get(10, TimeUnit.SECONDS))
    }

    sendMsgStart()

}
