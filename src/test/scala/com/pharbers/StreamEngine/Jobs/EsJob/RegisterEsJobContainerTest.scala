package com.pharbers.StreamEngine.Jobs.EsJob

import java.util.concurrent.TimeUnit

import com.pharbers.StreamEngine.Utils.Component.Dynamic.JobMsg
import com.pharbers.kafka.producer.PharbersKafkaProducer
import com.pharbers.kafka.schema.BPJob
import org.scalatest.FunSuite

class RegisterEsJobContainerTest extends FunSuite{
    test("register es job container") {

        import org.json4s._
        import org.json4s.jackson.Serialization.write
        implicit val formats: DefaultFormats.type = DefaultFormats
        val id = "57fe0-2bda-4880-8301-dc55a0"
        val traceId = "57fe0-2bda-4880-8301-dc55a0"
        val registerTopic = "stream_job_submit_jeo"
        val listeningTopic = "EsSinkJobSubmit"
        val `type` = "addList"
        val jobs = JobMsg(s"${id}", "job", "com.pharbers.StreamEngine.Jobs.EsJob.EsJobContainer.BPSEsSinkJobContainer", List("$BPSparkSession"), Nil, Nil, Map(
            "id" -> "57fe0-2bda-4880-8301-dc55a0",
            "listeningTopic" -> listeningTopic
        ), "", "es job") ::
            Nil

        val jobMsg = write(jobs)


        val pkp = new PharbersKafkaProducer[String, BPJob]
        val bpJob = new BPJob(id, traceId, `type`, jobMsg)
        val fu = pkp.produce(registerTopic, id, bpJob)
        println(fu.get(10, TimeUnit.SECONDS))

    }

    test("stop job"){
        import org.json4s._
        implicit val formats: DefaultFormats.type = DefaultFormats
        val id = "57fe0-2bda-4880-8301-dc55a0"
        val traceId = "57fe0-2bda-4880-8301-dc55a0"
        val `type` = "stop"
        val jobMsg = id

        val registerTopic = "stream_job_submit_jeo"

        val pkp = new PharbersKafkaProducer[String, BPJob]
        val bpJob = new BPJob(id, traceId, `type`, jobMsg)
        val fu = pkp.produce(registerTopic, id, bpJob)
        println(fu.get(10, TimeUnit.SECONDS))
    }

}
