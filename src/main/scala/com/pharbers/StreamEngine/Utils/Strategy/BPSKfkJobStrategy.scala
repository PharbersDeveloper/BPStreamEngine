package com.pharbers.StreamEngine.Utils.Strategy

import com.pharbers.StreamEngine.Utils.Annotation.Component
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Strategy.Session.Kafka.BPKafkaSession
import org.apache.kafka.common.config.ConfigDef

@Component(name = "BPSKfkJobStrategy", `type` = "strategy")
case class BPSKfkJobStrategy(kfk: BPKafkaSession) extends BPSJobStrategy {
    def getTopic: String = kfk.topic
    def getSchema: org.apache.spark.sql.types.DataType = kfk.sparkSchema

    override val componentProperty: Component2.BPComponentConfig = null
    override def createConfigDef(): ConfigDef = ???
}

object BPSKfkJobStrategy {
    def apply(kfk: BPKafkaSession, config: Map[String, String]): BPSKfkJobStrategy = new BPSKfkJobStrategy(kfk)
}
