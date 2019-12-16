package com.pharbers.StreamEngine.Utils.Kafka

import com.pharbers.kafka.producer.PharbersKafkaProducer
import org.apache.avro.specific.SpecificRecord

// TODO: 临时
object ProducerSingleton extends Serializable {
    var producer: PharbersKafkaProducer[String, SpecificRecord] = _
	def getIns: PharbersKafkaProducer[String, SpecificRecord] = {
		if (producer == null) {
			producer = new PharbersKafkaProducer[String, SpecificRecord]
		}
		producer
    }
}
