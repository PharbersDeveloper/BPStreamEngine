package com.pharbers.sandbox.kafka

import java.util.UUID
import java.util.concurrent.TimeUnit

import com.pharbers.StreamEngine.Utils.Component.Dynamic.JobMsg
import com.pharbers.kafka.producer.PharbersKafkaProducer
import com.pharbers.kafka.schema.BPJob
import org.scalatest.FunSuite

class PushJobTest extends FunSuite {
	test("push job") {
		import org.json4s._
		import org.json4s.jackson.Serialization.write
		implicit val formats: DefaultFormats.type = DefaultFormats
		
		val id = s"SandBoxJobContainer_${UUID.randomUUID().toString}"
		val jobId = ""
		val traceId = ""
		val `type` = "add"
		//        val jobMsg = write(JobMsg("testJob", "job", "com.pharbers.StreamEngine.Jobs.OssJob.DynamicJobDemo",
		//            List("$BPSparkSession", "$BPSKfkJobStrategy", "demo"), Nil, Nil, Map.empty, "", "test job"))
		//        val jobMsg = write(JobMsg("testListener", "listener", "com.pharbers.StreamEngine.Jobs.OssJob.DynamicListenerDemo",
		//            Nil, List("testJob"), List("id", "this"), Map.empty, "", "test listener"))
		//        val jobMsg = write(JobMsg("testListener2", "listener", "com.pharbers.StreamEngine.Jobs.OssJob.DynamicListenerDemo",
		//            List("another listener"), List("testJob"), List("this"), Map.empty, "", "test listener"))
		val jobMsg = write(JobMsg(id, "job", "com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxJobContainer.BPSSandBoxJobContainer",
			List("$BPSparkSession"), Nil, Nil, Map.empty, "", "SandBoxJob"))
		val topic = "stream_job_submit"
		
		val pkp = new PharbersKafkaProducer[String, BPJob]
		val bpJob = new BPJob(jobId, traceId, `type`, jobMsg)
		val fu = pkp.produce(topic, jobId, bpJob)
		println(fu.get(10, TimeUnit.SECONDS))
	}
}
