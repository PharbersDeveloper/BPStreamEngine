package com.pharbers.StreamEngine.BPStreamJob.FileJobs

import com.pharbers.StreamEngine.BPStreamJob.BPStreamJob
import com.pharbers.StreamEngine.BPStreamJob.FileJobs.OssListener.BPSOssListener
import com.pharbers.StreamEngine.BPStreamJob.JobStrategy.KfkJobStrategy
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BPSOssJob {
    def apply(strategy: KfkJobStrategy, spark: SparkSession): BPSOssJob = new BPSOssJob(strategy, spark)
}

class BPSOssJob(val strategy: KfkJobStrategy, val spark: SparkSession) extends BPStreamJob {
    type T = KfkJobStrategy
    import spark.implicits._
    val listener = new BPSOssListener(spark, this)

    override def open(): Unit = {
        val reading = spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "123.56.179.133:9092")
            .option("kafka.security.protocol", "SSL")
            .option("kafka.ssl.keystore.location", "./kafka.broker1.keystore.jks")
            .option("kafka.ssl.keystore.password", "pharbers")
            .option("kafka.ssl.truststore.location", "./kafka.broker1.truststore.jks")
            .option("kafka.ssl.truststore.password", "pharbers")
            .option("kafka.ssl.endpoint.identification.algorithm", " ")
            .option("subscribe", strategy.getTopic)
            .option("startingOffsets", "earliest")
            .load()

        inputStream = Some(reading
            .selectExpr(
                """deserialize(value) AS value""",
                "timestamp"
            ).toDF()
            .withWatermark("timestamp", "24 hours")
            .select(
                from_json($"value", strategy.getSchema).as("data")
            ).select("data.*"))
    }

    override def close(): Unit = inputStream match {
        case Some(is) => listener.deActive(is)
        case None => ???
    }

    override def exec(): Unit = inputStream match {
        case Some(is) => listener.active(is)
        case None => ???
    }
}
