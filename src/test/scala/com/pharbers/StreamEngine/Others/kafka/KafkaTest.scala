package com.pharbers.StreamEngine.Others.kafka

import org.json4s._
import org.scalatest.FunSuite
import java.util.concurrent.TimeUnit

import com.pharbers.kafka.schema.{BPJob, FileMetaData}
import org.json4s.jackson.Serialization.write
import com.pharbers.StreamEngine.Utils.Strategy.Session.Kafka.PharbersKafkaProducer
import com.pharbers.StreamEngine.Utils.Component.Dynamic.JobMsg

class KafkaTest extends FunSuite {

    test("send kafka message") {
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
        val topic = "null"

        val pkp = new PharbersKafkaProducer[String, BPJob]
        val bpJob = new BPJob(jobId, traceId, `type`, jobMsg)
        val fu = pkp.produce(topic, jobId, bpJob)
        println(fu.get(10, TimeUnit.SECONDS))
    }

    test("Avro Producer SandBox File Meta Data Test") {
        val gr = new FileMetaData()
        gr.setJobId("da0fb-c055-4d27-9d1a-fc9890")
        gr.setMetaDataPath("/test/alex/test001/metadata")
        gr.setSampleDataPath("/test/alex/test001/files/jobId=")
        gr.setConvertType("")
        val pkp = new PharbersKafkaProducer[String, FileMetaData]

        1 to 1 foreach { x =>
            val fu = pkp.produce("sb_file_meta_job_test_1", "value", gr)
            println(fu.get(10, TimeUnit.SECONDS))
        }
    }

    test("Avro Producer SandBox Convert Schema") {
        val gr = new FileMetaData()
        gr.setJobId("da0fb-c055-4d27-9d1a-fc9890")
        gr.setMetaDataPath("/test/alex/test001/metadata")
        gr.setSampleDataPath("/test/alex/test001/files/jobId=")
        gr.setConvertType("convert_schema")
        val pkp = new PharbersKafkaProducer[String, FileMetaData]

        1 to 1 foreach { x =>
            val fu = pkp.produce("sb_file_meta_job_test_1", "value", gr)
            println(fu.get(10, TimeUnit.SECONDS))
        }
    }
}
