package com.pharbers.StreamEngine.Utils.Config

import org.scalatest.FunSuite

class KafkaConfigTest extends FunSuite {

    test("kafka config define") {
        println("start app config define")

        val ac = KafkaConfig()

        val p = ac.getString(KafkaConfig.KAFKA_BOOTSTRAP_SERVERS_KEY)
        val h = ac.getString(KafkaConfig.KAFKA_SECURITY_PROTOCOL_KEY)

        println("server=" + p)
        println("protocol=" + h)

    }
}
