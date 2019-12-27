package com.pharbers.StreamEngine.Jobs.EsJob

import java.util.UUID
import java.util.concurrent.TimeUnit
import com.pharbers.kafka.producer.PharbersKafkaProducer
import com.pharbers.kafka.schema.EsSinkJobSubmit
import org.scalatest.FunSuite

class StartEsJobTest extends FunSuite{

    test("push jobs") {
        import org.json4s._
        implicit val formats: DefaultFormats.type = DefaultFormats
        val jobId = UUID.randomUUID().toString

        val indexName = "test1"
        val metadataPath = "/user/alex/jobs/6cdb14ff-7897-4bd8-b9c5-7785c98935d1/78c523b3-29ed-4cbd-a59d-b5a9ddec6c47/metadata/cbc16420-577d-4a48-907e-f8347f5cfb67"
        val filesPath = "/user/alex/jobs/6cdb14ff-7897-4bd8-b9c5-7785c98935d1/78c523b3-29ed-4cbd-a59d-b5a9ddec6c47/contents/cbc16420-577d-4a48-907e-f8347f5cfb67"

        val topic = "EsSinkJobSubmit"

        val pkp = new PharbersKafkaProducer[String, EsSinkJobSubmit]
        val esJob = new EsSinkJobSubmit(indexName, metadataPath, filesPath)
        val fu = pkp.produce(topic, jobId, esJob)
        println(fu.get(10, TimeUnit.SECONDS))
    }

}
