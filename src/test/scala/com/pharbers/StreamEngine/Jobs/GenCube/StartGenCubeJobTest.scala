package com.pharbers.StreamEngine.Jobs.GenCube

import java.util.UUID
import java.util.concurrent.TimeUnit

import com.pharbers.StreamEngine.Jobs.GenCubeJob.BPSGenCubeJob
import com.pharbers.kafka.producer.PharbersKafkaProducer
import com.pharbers.kafka.schema.GenCubeJobSubmit
import org.scalatest.FunSuite

class StartGenCubeJobTest extends FunSuite{

    test("push jobs") {
        import org.json4s._
        implicit val formats: DefaultFormats.type = DefaultFormats
        val jobId = UUID.randomUUID().toString

        val topic = "GenCubeJobSubmit"

        val inputDataType = BPSGenCubeJob.HIVE_DATA_TYPE
        val inputPath = "SELECT * FROM result"
        val outputDataType = "es"
        val outputPath = "fullcube"
        val strategy = BPSGenCubeJob.STRATEGY_CMD_HANDLE_HIVE_RESULT

        val genCubeJob = new GenCubeJobSubmit(inputDataType, inputPath, outputDataType, outputPath, strategy)

        val pkp = new PharbersKafkaProducer[String, GenCubeJobSubmit]
        val fu = pkp.produce(topic, jobId, genCubeJob)

        println(fu.get(10, TimeUnit.SECONDS))
    }

}
