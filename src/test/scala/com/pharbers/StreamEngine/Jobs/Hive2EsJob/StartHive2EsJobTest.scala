package com.pharbers.StreamEngine.Jobs.Hive2EsJob

import java.util.UUID
import java.util.concurrent.TimeUnit
import com.pharbers.StreamEngine.Utils.Strategy.Session.Kafka.PharbersKafkaProducer
import com.pharbers.kafka.schema.Hive2EsJobSubmit
import org.scalatest.FunSuite

class StartHive2EsJobTest extends FunSuite{

    test("push jobs") {
        import org.json4s._
        implicit val formats: DefaultFormats.type = DefaultFormats
        val jobId = UUID.randomUUID().toString

        val sql = "SELECT * FROM result"
        val indexName = "result"
        val strategy = BPSHive2EsJob.STRATEGY_CMD_MaxDashboard
        val topic = "Hive2EsJobSubmit"


        val pkp = new PharbersKafkaProducer[String, Hive2EsJobSubmit]
        val esJob = new Hive2EsJobSubmit(sql, indexName, strategy)
        val fu = pkp.produce(topic, jobId, esJob)
        println(fu.get(10, TimeUnit.SECONDS))
    }

}
