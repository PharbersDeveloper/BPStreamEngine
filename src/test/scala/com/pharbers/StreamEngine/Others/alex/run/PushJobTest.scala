package com.pharbers.StreamEngine.Others.alex.run

import org.scalatest.FunSuite

class PushJobTest extends FunSuite {
	test("push job") {
//		import org.json4s._
//		import org.json4s.jackson.Serialization.write
//		implicit val formats: DefaultFormats.type = DefaultFormats
//		val jobId = "201910231514"
//		val traceId = "201910231514"
//		val `type` = "addList"
//		val jobs = JobMsg("ossStreamJob", "job", "com.pharbers.StreamEngine.Jobs.OssPartitionJob.OssJobContainer.BPSOssPartitionJobContainer", List("$BPSKfkJobStrategy", "$BPSparkSession"), Nil, Nil, Map.empty, "", "oss job") ::
//			JobMsg("sandBoxJob", "job", "com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxJobContainer.BPSSandBoxJobContainer", List("$BPSparkSession"), Nil, Nil, Map.empty, "", "sandbox job") ::
//			Nil
//
//		val jobMsg = write(jobs)
//		val topic = "stream_job_submit"
//
//		val pkp = new PharbersKafkaProducer[String, BPJob]
//		val bpJob = new BPJob(jobId, traceId, `type`, jobMsg)
//		val fu = pkp.produce(topic, jobId, bpJob)
//		println(fu.get(10, TimeUnit.SECONDS))
		// 5ddf51a59c0809358c976752
//		import org.mongodb.scala.bson.ObjectId
//		println(new ObjectId("5ddf51a59c0809358c976752").toString)
		
//		val uid = UUID.randomUUID().toString
//		val mongoId = new ObjectId().toHexString
//		println(mongoId)
//		val dfs = new DataSet(Collections.emptyList(),
//			mongoId,
//			uid,
//			Collections.emptyList(),
//			"",
//			0,
//			"url",
//			"")
//		BPSBloodJob(uid, "data_set_job", dfs).exec()
		
//		val uploadEnd = new UploadEnd("5ddf6bd38da6b140b3766d6a", "08300-8033-494a-9cb5-3acee")
//		BPSUploadEndJob("", "upload_end_job", uploadEnd).exec()
	}
}
