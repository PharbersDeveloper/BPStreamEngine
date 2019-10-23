package com.pharbers.StreamEngine.Utils.Config

import java.io.FileInputStream
import java.util
import java.util.{Map, Properties}

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

object KafkaConfig  {

    final private val CD: ConfigDef = baseConfigDef
    final val PROPS: Map[_, _] = baseProps

    final private val CONFIG_PATH_KEY = "kafka.config.path"
    final private val DEFAULT_CONFIG_PATH = "resources/kafka.properties"

    final val KAFKA_BOOTSTRAP_SERVERS_KEY = "kafka.bootstrap.servers"
    final private val KAFKA_BOOTSTRAP_SERVERS_DOC = "kafka bootstrap servers."

    final val KAFKA_SECURITY_PROTOCOL_KEY = "kafka.security.protocol"
    final private val KAFKA_SECURITY_PROTOCOL_DOC = "kafka security protocol."

    final val KAFKA_SSL_KEYSTORE_LOCATION_KEY = "kafka.ssl.keystore.location"
    final private val KAFKA_SSL_KEYSTORE_LOCATION_DOC = "kafka ssl keystore location."

    final val KAFKA_SSL_KEYSTORE_PASSWORD_KEY = "kafka.ssl.keystore.password"
    final private val KAFKA_SSL_KEYSTORE_PASSWORD_DOC = "kafka ssl keystore password."

    final val KAFKA_SSL_TRUSTSTORE_LOCATION_KEY = "kafka.ssl.truststore.location"
    final private val KAFKA_SSL_TRUSTSTORE_LOCATION_DOC = "kafka ssl truststore location."

    final val KAFKA_SSL_TRUSTSTORE_PASSWORD_KEY = "kafka.ssl.truststore.password"
    final private val KAFKA_SSL_TRUSTSTORE_PASSWORD_DOC = "kafka ssl truststore password."

    final val KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_KEY = "kafka.ssl.endpoint.identification.algorithm"
    final private val KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC = "kafka ssl endpoint identification algorithm."

    final val KAFKA_STARTING_OFFSETS_KEY = "startingOffsets"
    final private val KAFKA_STARTING_OFFSETS_DOC = "kafka message starting offsets."

    private val c = new KafkaConfig(CD, PROPS)
    def apply(): KafkaConfig = c

    def apply(props: Map[_, _]): KafkaConfig = new KafkaConfig(CD, props)

    private def baseConfigDef: ConfigDef = {
        new ConfigDef()
            .define(
                KAFKA_BOOTSTRAP_SERVERS_KEY,
                Type.STRING,
                Importance.HIGH,
                KAFKA_BOOTSTRAP_SERVERS_DOC)
            .define(
                KAFKA_SECURITY_PROTOCOL_KEY,
                Type.STRING,
                Importance.HIGH,
                KAFKA_SECURITY_PROTOCOL_DOC)
            .define(
                KAFKA_SSL_KEYSTORE_LOCATION_KEY,
                Type.STRING,
                Importance.HIGH,
                KAFKA_SSL_KEYSTORE_LOCATION_DOC)
            .define(
                KAFKA_SSL_KEYSTORE_PASSWORD_KEY,
                Type.STRING,
                Importance.HIGH,
                KAFKA_SSL_KEYSTORE_PASSWORD_DOC)
            .define(
                KAFKA_SSL_TRUSTSTORE_LOCATION_KEY,
                Type.STRING,
                Importance.HIGH,
                KAFKA_SSL_TRUSTSTORE_LOCATION_DOC)
            .define(
                KAFKA_SSL_TRUSTSTORE_PASSWORD_KEY,
                Type.STRING,
                Importance.HIGH,
                KAFKA_SSL_TRUSTSTORE_PASSWORD_DOC)
            .define(
                KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_KEY,
                Type.STRING,
                Importance.HIGH,
                KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC)
            .define(
                KAFKA_STARTING_OFFSETS_KEY,
                Type.STRING,
                Importance.HIGH,
                KAFKA_STARTING_OFFSETS_DOC)
    }

    private def baseProps: Map[_, _] = {
        val appConfigPath: String = sys.env.getOrElse(CONFIG_PATH_KEY, DEFAULT_CONFIG_PATH)
        val props = new Properties()
        props.load(new FileInputStream(appConfigPath))
        props
    }

}

class KafkaConfig(definition: ConfigDef, originals: util.Map[_, _]) extends AbstractConfig(definition, originals) {

}
