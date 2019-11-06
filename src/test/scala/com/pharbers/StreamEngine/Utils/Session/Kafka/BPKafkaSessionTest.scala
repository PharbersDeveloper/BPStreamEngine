package com.pharbers.StreamEngine.Utils.Session.Kafka

import org.scalatest.FunSuite
import com.pharbers.StreamEngine.Utils.Session.Spark.BPSparkSession

/** Kafka Session Test
 *
 * @author clock
 * @version 0.1
 * @since 2019/11/06 11:57
 * @note 注意配置文件的优先级和使用方式
 */
class BPKafkaSessionTest extends FunSuite {
    test("Create BPKafkaSession By Default Config ") {
        val spark = BPSparkSession()
        val session = BPKafkaSession(spark, Map.empty)
        assert(session.kafkaUrl == session.kafkaUrl)
        assert(session.schemaRegistryUrl == session.schemaRegistryUrl)
        assert(session.topic == session.topic)
        assert(session.sparkSchema != null)
    }

    test("Create BPKafkaSession By Args Config ") {
        val spark = BPSparkSession()
        val session = BPKafkaSession(spark, Map("topic" -> "testTopic"))
        assert(session.kafkaUrl == session.kafkaUrl)
        assert(session.schemaRegistryUrl == session.schemaRegistryUrl)
        assert(session.topic == "testTopic")
        assert(session.sparkSchema == null)
    }
}
