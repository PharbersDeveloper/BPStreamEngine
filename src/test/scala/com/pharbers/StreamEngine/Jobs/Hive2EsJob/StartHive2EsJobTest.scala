package com.pharbers.StreamEngine.Jobs.Hive2EsJob

import java.util.UUID
import java.util.concurrent.TimeUnit

import com.pharbers.kafka.producer.PharbersKafkaProducer
import com.pharbers.kafka.schema.Hive2EsJobSubmit
import org.scalatest.FunSuite

class StartHive2EsJobTest extends FunSuite{

    test("push jobs") {
        import org.json4s._
        implicit val formats: DefaultFormats.type = DefaultFormats
        val jobId = UUID.randomUUID().toString

        val sql = "SELECT COMPANY, SOURCE, PROVINCE_NAME, CITY_NAME, HOSP_NAME, HOSP_CODE, HOSP_LEVEL, MOLE_NAME, PRODUCT_NAME, MANUFACTURER_NAME, MKT," +
            " CAST(SALES_VALUE As DOUBLE) AS SALES, CAST(YEAR As INT) AS YEAR, CAST(MONTH As INT) AS MONTH" +
            " FROM cpa WHERE ( YEAR like '2018%' AND MONTH >= 8 ) OR ( YEAR like '2019%' AND MONTH <= 8 )"
        val indexName = "cpa"
        val strategy = BPSHive2EsJob.STRATEGY_CMD_MaxDashboard
        val topic = "Hive2EsJobSubmit"


        val pkp = new PharbersKafkaProducer[String, Hive2EsJobSubmit]
        val esJob = new Hive2EsJobSubmit(sql, indexName, strategy)
        val fu = pkp.produce(topic, jobId, esJob)
        println(fu.get(10, TimeUnit.SECONDS))
    }

}
