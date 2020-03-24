package com.pharbers.StreamEngine.Jobs.KfkSinkJob
import java.util.UUID

import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Job.{BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Utils.Strategy.{BPStrategyComponent, BPSKfkBaseStrategy}
import org.apache.kafka.common.config.ConfigDef
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConversions.mapAsScalaMap

object BPSKfkSinkJob {
    // TODO: concert是这个job自己的一些列不一样的参数
    // strategy是kafka的参数，从父亲继承而来
    def apply(
                 id: String,
                 spark: SparkSession,
                 inputStream: Option[sql.DataFrame],
                 container: BPSJobContainer,
                 strategy: BPSKfkBaseStrategy,
                 concert: Map[String, Any]
             ): BPSKfkSinkJob =
        new BPSKfkSinkJob(id, spark, inputStream, container, strategy, concert)
}

class BPSKfkSinkJob(
                       val id: String,
                       val spark: SparkSession,
                       val is: Option[sql.DataFrame],
                       val container: BPSJobContainer,
                       override val strategy: BPSKfkBaseStrategy,
                       concert: Map[String, Any]) extends BPStreamJob {
    type T = BPStrategyComponent

    override def exec(): Unit = {
        inputStream = is

        lazy val topic: String = "topic" // concert.apply("topic").toString // TODO: 缺少抽象
        inputStream match {
            case Some(output) => {
                output.writeStream
                    .format("kafka")
                    // TODO: 兼容参数化方案，公用Kafka Strategy
//                    .options(mapAsScalaMap(KafkaConfig.PROPS).map(x => (x._1.toString, x._2.toString)))
                    .option("kafka.bootstrap.servers", "123.56.179.133:9092")
                    .option("kafka.security.protocol", "SSL")
                    .option("kafka.ssl.keystore.location", "./kafka.broker1.keystore.jks")
                    .option("kafka.ssl.keystore.password", "pharbers")
                    .option("kafka.ssl.truststore.location", "./kafka.broker1.truststore.jks")
                    .option("kafka.ssl.truststore.password", "pharbers")
                    .option("kafka.ssl.endpoint.identification.algorithm", " ")
                    .option("topic", s"${topic}")
                    .option("checkpointLocation", "/test/streaming/" + this.id + "/checkpoint")
                    .start()
            }
        }
    }

    override def close(): Unit = {
        super.close()
        container.finishJobWithId(id)
    }

    override val componentProperty: Component2.BPComponentConfig = null

    override def createConfigDef(): ConfigDef = ???
}
