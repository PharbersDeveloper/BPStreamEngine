package com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy

import com.pharbers.StreamEngine.Utils.Session.Kafka.BPKafkaSession

case class BPSKfkJobStrategy(kfk: BPKafkaSession) extends BPSJobStrategy {
    override def getTopic: String = kfk.topic
    override def getSchema: org.apache.spark.sql.types.DataType = kfk.sparkSchema
}
