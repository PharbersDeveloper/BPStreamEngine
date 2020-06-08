package com.pharbers.StreamEngine.Run

import org.json4s._
import java.util.UUID
import org.scalatest.FunSuite
import java.util.concurrent.TimeUnit
import com.pharbers.kafka.schema.BPJob
import org.json4s.jackson.Serialization.write
import org.apache.avro.specific.SpecificRecord
import com.pharbers.StreamEngine.Utils.Strategy.Session.Kafka.PharbersKafkaProducer
import com.pharbers.StreamEngine.Utils.Component.Dynamic.JobMsg

class LaunchContainers extends FunSuite {

    test("launch containers") {
        implicit val formats: DefaultFormats.type = DefaultFormats

        val jobId = "2020-0324-0001"
        val traceId = "2020-0324-0001"
        val `type` = "addList"

        val jobs =
            JobMsg("ossStreamJob", "job",
                "com.pharbers.StreamEngine.Jobs.OssPartitionJob.OssJobContainer.BPSOssPartitionJobContainer",
                List("$BPSKfkJobStrategy", "$BPSparkSession"), Nil, Nil,
                Map.empty,
                "", "oss job"
            ) :: JobMsg("sandBoxJob", "job",
                "com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxJobContainer.BPSSandBoxJobContainer",
                List("$BPSparkSession"), Nil, Nil,
                Map.empty,
                "", "sandbox job"
            ) :: JobMsg("BPSPythonJobContainer", "job",
                "com.pharbers.StreamEngine.Jobs.PyJob.PythonJobContainer.BPSPythonJobContainer",
                List("$BPSparkSession"), Nil, Nil,
                Map(
                    "listenerTopic" -> "PyJobContainerListenerTopic",
                    "defaultNoticeTopic" -> "HiveTask",
                    "pythonUri" -> "https://github.com/PharbersDeveloper/bp-data-clean.git",
                    "pythonBranch" -> "v0.0.1"
                ),
                "", "python clean job"
            ) :: Nil

        val jobMsg = write(jobs)
        val topic = "stream_job_submit"

        val pkp = new PharbersKafkaProducer[String, BPJob]
        val bpJob = new BPJob(jobId, traceId, `type`, jobMsg)
        val fu = pkp.produce(topic, jobId, bpJob)
        println(fu.get(10, TimeUnit.SECONDS))
    }

    test("close container") {
        import org.json4s._
        implicit val formats: DefaultFormats.type = DefaultFormats

        val jobId = "2020-0324-0001"
        val traceId = "2020-0324-0001"
        val `type` = "stop"
        val jobMsg = "SandBoxJobContainer_6260762d-5c7a-495f-be09-70128d8a440b"

        val topic = "stream_job_submit"

        val pkp = new PharbersKafkaProducer[String, BPJob]
        val bpJob = new BPJob(jobId, traceId, `type`, jobMsg)
        val fu = pkp.produce(topic, jobId, bpJob)
        println(fu.get(10, TimeUnit.SECONDS))
    }
}
