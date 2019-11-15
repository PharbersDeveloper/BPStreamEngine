package com.pharbers.StreamEngine.Others.sandbox.run

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
		val jobId = "201910231514"
		val traceId = "201910231514"
		val `type` = "addList"
		val jobs = JobMsg("ossStreamJob", "job", "com.pharbers.StreamEngine.Jobs.OssPartitionJob.OssJobContainer.BPSOssPartitionJobContainer", List("$BPSKfkJobStrategy", "$BPSparkSession"), Nil, Nil, Map.empty, "", "oss job") ::
			JobMsg("sandBoxJob", "job", "com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxJobContainer.BPSSandBoxJobContainer", List("$BPSparkSession"), Nil, Nil, Map.empty, "", "sandbox job") ::
			Nil
		
		val jobMsg = write(jobs)
		val topic = "stream_job_submit"
		
		val pkp = new PharbersKafkaProducer[String, BPJob]
		val bpJob = new BPJob(jobId, traceId, `type`, jobMsg)
		val fu = pkp.produce(topic, jobId, bpJob)
		println(fu.get(10, TimeUnit.SECONDS))
	}
}
