package com.pharbers.StreamEngine.Jobs.PyJob

import org.json4s._
import org.scalatest.FunSuite
import java.util.concurrent.TimeUnit
import com.pharbers.kafka.schema.BPJob
import org.json4s.jackson.Serialization.write
import com.pharbers.kafka.producer.PharbersKafkaProducer
import com.pharbers.StreamEngine.Utils.Component.Dynamic.JobMsg

class BPSPythonJobContainerTest extends FunSuite {

    test("send kafka message open BPSPythonJobContainer") {
        val jobId = "202003230001"
        val traceId = "202003230001"
        val `type` = "add"

        val jobs = JobMsg(
            id = "BPSPythonJobContainer",
            `type` = "job",
            classPath = "com.pharbers.StreamEngine.Jobs.PyJob.PythonJobContainer.BPSPythonJobContainer",
            args = List("$BPSparkSession"),
            dependencies = Nil,
            dependencyArgs = Nil,
            config = Map(
                "listenerTopic" -> "PyJobContainerListenerTopic",
                "defaultNoticeTopic" -> "HiveTask",
                "pythonUri" -> "https://github.com/PharbersDeveloper/bp-data-clean.git",
                "pythonBranch" -> "v0.0.1"
            ),
            dependencyStop = "",
            description = "python clean job"
        ) :: Nil

        implicit val formats: DefaultFormats.type = DefaultFormats
        val jobMsg = write(jobs)
        val topic = "stream_job_submit_pyjob"

        val pkp = new PharbersKafkaProducer[String, BPJob]
        val bpJob = new BPJob(jobId, traceId, `type`, jobMsg)
        val fu = pkp.produce(topic, jobId, bpJob)
        println(fu.get(10, TimeUnit.SECONDS))
    }

    test("send kafka message close BPSPythonJobContainer") {
        val jobId = "202003230001"
        val traceId = "202003230001"
        val `type` = "stop"

        val jobMsg = "BPSPythonJobContainer_16abc7d1-f1ca-4f75-8bd3-9e35c133c440"

        val topic = "stream_job_submit_pyjob"

        val pkp = new PharbersKafkaProducer[String, BPJob]
        val bpJob = new BPJob(jobId, traceId, `type`, jobMsg)
        val fu = pkp.produce(topic, jobId, bpJob)
        println(fu.get(10, TimeUnit.SECONDS))
    }

    test("send kafka message exec BPSPythonJob") {
        val runId = "runid-202003230001"

        val job = Map(
            "parentsId" -> "parentsId",
            "noticeTopic" -> "HiveTaskNone",
            "metadataPath" -> "/jobs/02c07385-39fa-496a-a9ac-029ed09aa79c/0db59660-056f-4d48-9ade-90b8ceaadc57/metadata",
            "filesPath" -> "/jobs/02c07385-39fa-496a-a9ac-029ed09aa79c/0db59660-056f-4d48-9ade-90b8ceaadc57/contents",
            "resultPath" -> s"./jobs/$runId"
        )

        implicit val formats: DefaultFormats.type = DefaultFormats
        val jobMsg = write(job)
        val topic = "PyJobContainerListenerTopic"

        val pkp = new PharbersKafkaProducer[String, BPJob]
        val bpJob = new BPJob("", "", "", jobMsg)
        val fu = pkp.produce(topic, runId, bpJob)
        println(fu.get(10, TimeUnit.SECONDS))
    }
}
