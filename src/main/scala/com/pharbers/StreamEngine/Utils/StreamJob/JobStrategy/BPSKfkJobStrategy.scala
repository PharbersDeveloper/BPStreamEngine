package com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy

import com.pharbers.StreamEngine.Utils.Annotation.Component
import com.pharbers.StreamEngine.Utils.Session.Kafka.BPKafkaSession

@Component(name = "BPSKfkJobStrategy", `type` = "strategy")
case class BPSKfkJobStrategy(kfk: BPKafkaSession) extends BPSJobStrategy {
    override def getTopic: String = kfk.topic
    override def getSchema: org.apache.spark.sql.types.DataType = kfk.sparkSchema
}

object BPSKfkJobStrategy {
    def apply(kfk: BPKafkaSession, config: Map[String, String]): BPSKfkJobStrategy = new BPSKfkJobStrategy(kfk)
}
