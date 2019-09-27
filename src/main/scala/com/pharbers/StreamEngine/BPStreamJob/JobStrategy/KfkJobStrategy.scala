package com.pharbers.StreamEngine.BPStreamJob.JobStrategy

import com.pharbers.StreamEngine.BPKafkaSession.BPKafkaSession

case class KfkJobStrategy(kfk: BPKafkaSession) extends JobStrategy {
    override def getTopic: String = kfk.topic
    override def getSchema: org.apache.spark.sql.types.DataType = kfk.sparkSchema
}
