package com.pharbers.StreamEngine.Jobs.PyJob

import org.json4s._
import java.util.UUID
import org.scalatest.FunSuite
import java.util.concurrent.TimeUnit
import com.pharbers.kafka.schema.BPJob
import org.json4s.jackson.Serialization.write
import com.pharbers.kafka.producer.PharbersKafkaProducer
import com.pharbers.StreamEngine.Utils.Component.Dynamic.JobMsg

class RunPyJob extends FunSuite {
    implicit val formats: DefaultFormats.type = DefaultFormats

    test("start python container") {
        val containerId = UUID.randomUUID().toString
        println(containerId)

        val jobId = containerId
        val traceId = "traceId_" + containerId
        val topic = "stream_job_submit_qi"

        val jobMsg = write(JobMsg(
            id = containerId,
            `type` = "job",
            classPath = "com.pharbers.StreamEngine.Jobs.PyJob.PythonJobContainer.BPSPythonJobContainer",
            args = List("$BPSparkSession"),
            dependencies = Nil,
            dependencyArgs = Nil,
            config = Map(
                "containerId" -> containerId,
                "listenerTopic" -> "PyJobContainerListenerTopic",
                "defaultNoticeTopic" -> "PyJobContainerDefaultNoticeTopic",
                "defaultPartition" -> "4",
                "defaultRetryCount" -> "4",
                "pythonUri" -> "https://github.com/PharbersDeveloper/bp-data-clean.git",
                "pythonBranch" -> "v0.0.1"
            ),
            dependencyStop = "",
            description = "start python container"
        ))

        val bpJob = new BPJob(jobId, traceId, "add", jobMsg)
        val pkp = new PharbersKafkaProducer[String, BPJob]
        val fu = pkp.produce(topic, jobId, bpJob)
        println(fu.get(10, TimeUnit.SECONDS))
    }

    test("start python clean job") {
        val jobId = UUID.randomUUID().toString
        val topic = "PyJobContainerListenerTopic"
        val traceId = "traceId_" + jobId

        val jobMsg = write(Map(
            "jobId" -> jobId,
            "metadataPath" -> "hdfs:///user/alex/jobs/94350fa8-420e-4f5f-8f1e-467627aafec3/ff94c3bd-1887-4800-a5f4-43a292869008/metadata",
            "filesPath" -> "hdfs:///user/alex/jobs/94350fa8-420e-4f5f-8f1e-467627aafec3/ff94c3bd-1887-4800-a5f4-43a292869008/contents/6f6b8e34-88df-4bef-a930-20197ccd6ef0",
            "resultPath" -> "./jobs/"
        ))

        val pkp = new PharbersKafkaProducer[String, BPJob]
        val bpJob = new BPJob(jobId, traceId, "add", jobMsg)
        val fu = pkp.produce(topic, jobId, bpJob)
        println(fu.get(10, TimeUnit.SECONDS))
    }
}
