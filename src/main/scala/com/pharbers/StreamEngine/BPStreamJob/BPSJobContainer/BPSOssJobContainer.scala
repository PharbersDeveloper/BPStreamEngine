package com.pharbers.StreamEngine.BPStreamJob.BPSJobContainer

import java.util.UUID

import com.pharbers.StreamEngine.BPStreamJob.BPStreamJob
import com.pharbers.StreamEngine.BPStreamJob.FileJobs.BPSOssJob
import com.pharbers.StreamEngine.BPStreamJob.FileJobs.OssListener.BPSOssListener
import com.pharbers.StreamEngine.BPStreamJob.FileJobs.OssListenerV2.BPSOssListenerV2
import com.pharbers.StreamEngine.BPStreamJob.JobStrategy.KfkJobStrategy
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BPSOssJobContainer {
    def apply(strategy: KfkJobStrategy, spark: SparkSession): BPSOssJobContainer = new BPSOssJobContainer(strategy, spark)
}

class BPSOssJobContainer(override val strategy: KfkJobStrategy, val spark: SparkSession) extends BPSJobContainer {
    val id = UUID.randomUUID().toString
    type T = KfkJobStrategy
    import spark.implicits._
    //    val listener = new BPSOssListener(spark, this)

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
                from_json($"value", strategy.getSchema).as("data"), col("timestamp")
            ).select("data.*", "timestamp"))
    }

//    override def close(): Unit = {
//        println("alfred clean all the job ========>")
//        handlers.foreach(_.close())
//        listeners.foreach(_.deActive())
//        outputStream.foreach(_.stop())
//        inputStream match {
//            case Some(is) =>
//            case None => ???
//        }
//    }

    override def exec(): Unit = inputStream match {
        case Some(is) => {
            val listener = new BPSOssListenerV2(spark, this)
            listener.active(is)
            listeners = listener :: listeners
        }
        case None => ???
    }

    override def getJobWithId(id: String, category: String = ""): BPStreamJob = {
        jobs.get(id) match {
            case Some(job) => job
            case None => {
                val job = BPSOssJob(id, spark, this.inputStream, this)
                job.exec()
                jobs += id -> job
                job
            }
        }
    }
}
