package com.pharbers.StreamEngine.Jobs.OutputJob
import java.util.UUID

import org.apache.spark.sql.{DataFrame, SparkSession}

class KafkaOutputJob extends OutputJob {

    lazy val topic: String = "bps_output"

    override def sink(output: DataFrame): Unit = {

        output.writeStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "123.56.179.133:9092")
            .option("kafka.security.protocol", "SSL")
            .option("kafka.ssl.keystore.location", "./kafka.broker1.keystore.jks")
            .option("kafka.ssl.keystore.password", "pharbers")
            .option("kafka.ssl.truststore.location", "./kafka.broker1.truststore.jks")
            .option("kafka.ssl.truststore.password", "pharbers")
            .option("kafka.ssl.endpoint.identification.algorithm", " ")
            .option("topic", s"${topic}")
            .option("checkpointLocation", "/test/streaming/" + UUID.randomUUID().toString + "/checkpoint")
            .start()

    }

}
