package com.pharbers.StreamEngine.Jobs.GenCube

import java.util.concurrent.TimeUnit

import com.pharbers.StreamEngine.Utils.Component.Dynamic.JobMsg
import com.pharbers.StreamEngine.Utils.Strategy.Session.Kafka.PharbersKafkaProducer
import com.pharbers.kafka.schema.BPJob
import org.scalatest.FunSuite

class RegisterGenCubeJobContainerTest extends FunSuite{
    test("register gen-cube job container") {

        import org.json4s._
        import org.json4s.jackson.Serialization.write
        implicit val formats: DefaultFormats.type = DefaultFormats
        val id = "57fe0-2bda-4880-8301-dc55a1"
        val traceId = "57fe0-2bda-4880-8301-dc55a1"
        val registerTopic = "stream_job_submit_jeo"
        val listeningTopic = "GenCubeJobSubmit"
        val `type` = "addList"
        val jobs = JobMsg(s"${id}", "job", "com.pharbers.StreamEngine.Jobs.GenCubeJob.BPSGenCubeJobContainer", List("$BPSparkSession"), Nil, Nil, Map(
            "container.id" -> id,
            "container.listening.topic" -> listeningTopic
        ), "", "hive to es job") ::
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
        val id = "57fe0-2bda-4880-8301-dc55a1"
        val traceId = "57fe0-2bda-4880-8301-dc55a1"
        val `type` = "stop"
        val jobMsg = id

        val registerTopic = "stream_job_submit_jeo"

        val pkp = new PharbersKafkaProducer[String, BPJob]
        val bpJob = new BPJob(id, traceId, `type`, jobMsg)
        val fu = pkp.produce(registerTopic, id, bpJob)
        println(fu.get(10, TimeUnit.SECONDS))
    }

}
