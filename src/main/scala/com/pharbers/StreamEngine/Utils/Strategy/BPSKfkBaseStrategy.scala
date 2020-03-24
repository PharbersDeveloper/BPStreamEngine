package com.pharbers.StreamEngine.Utils.Strategy

import com.pharbers.StreamEngine.Utils.Annotation.Component
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Strategy.Session.Kafka.BPKafkaSession
import org.apache.kafka.common.config.ConfigDef

@Component(name = "BPSKfkJobStrategy", `type` = "strategy")
case class BPSKfkBaseStrategy(kfk: BPKafkaSession) extends BPStrategyComponent {
    def getTopic: String = kfk.topic
    def getSchema: org.apache.spark.sql.types.DataType = kfk.sparkSchema

    override val componentProperty: Component2.BPComponentConfig = null
    override def createConfigDef(): ConfigDef = ???

    override val strategyName: String = "kafka strategy"
}

object BPSKfkBaseStrategy {
    def apply(kfk: BPKafkaSession, config: Map[String, String]): BPSKfkBaseStrategy = new BPSKfkBaseStrategy(kfk)
}
