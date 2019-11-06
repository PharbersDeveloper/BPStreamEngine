package com.pharbers.StreamEngine.Utils.Config

import java.io.FileInputStream
import java.util
import java.util.{Map, Properties}

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

object KafkaConfig  {
    val kafkaConfigEnvPath: String = "kafka.config.path"
    val defaultKafkaConfigsPath: String = "src/main/resources/kafka.properties"

    final val KAFKA_BOOTSTRAP_SERVERS_KEY = "kafka.bootstrap.servers"
    final val KAFKA_BOOTSTRAP_SERVERS_DOC = "kafka bootstrap servers."

    final val KAFKA_SECURITY_PROTOCOL_KEY = "kafka.security.protocol"
    final val KAFKA_SECURITY_PROTOCOL_DOC = "kafka security protocol."

    final val KAFKA_SSL_KEYSTORE_LOCATION_KEY = "kafka.ssl.keystore.location"
    final val KAFKA_SSL_KEYSTORE_LOCATION_DOC = "kafka ssl keystore location."

    final val KAFKA_SSL_KEYSTORE_PASSWORD_KEY = "kafka.ssl.keystore.password"
    final val KAFKA_SSL_KEYSTORE_PASSWORD_DOC = "kafka ssl keystore password."

    final val KAFKA_SSL_TRUSTSTORE_LOCATION_KEY = "kafka.ssl.truststore.location"
    final val KAFKA_SSL_TRUSTSTORE_LOCATION_DOC = "kafka ssl truststore location."

    final val KAFKA_SSL_TRUSTSTORE_PASSWORD_KEY = "kafka.ssl.truststore.password"
    final val KAFKA_SSL_TRUSTSTORE_PASSWORD_DOC = "kafka ssl truststore password."

    final val KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_KEY = "kafka.ssl.endpoint.identification.algorithm"
    final val KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC = "kafka ssl endpoint identification algorithm."

    final val KAFKA_STARTING_OFFSETS_KEY = "startingOffsets"
    final val KAFKA_STARTING_OFFSETS_DOC = "kafka message starting offsets."

    private def configDef: ConfigDef = new ConfigDef()
            .define(KAFKA_BOOTSTRAP_SERVERS_KEY, Type.STRING, Importance.HIGH, KAFKA_BOOTSTRAP_SERVERS_DOC)
            .define(KAFKA_SECURITY_PROTOCOL_KEY, Type.STRING, Importance.HIGH, KAFKA_SECURITY_PROTOCOL_DOC)
            .define(KAFKA_SSL_KEYSTORE_LOCATION_KEY, Type.STRING, Importance.HIGH, KAFKA_SSL_KEYSTORE_LOCATION_DOC)
            .define(KAFKA_SSL_KEYSTORE_PASSWORD_KEY, Type.STRING, Importance.HIGH, KAFKA_SSL_KEYSTORE_PASSWORD_DOC)
            .define(KAFKA_SSL_TRUSTSTORE_LOCATION_KEY, Type.STRING, Importance.HIGH, KAFKA_SSL_TRUSTSTORE_LOCATION_DOC)
            .define(KAFKA_SSL_TRUSTSTORE_PASSWORD_KEY, Type.STRING, Importance.HIGH, KAFKA_SSL_TRUSTSTORE_PASSWORD_DOC)
            .define(KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_KEY, Type.STRING, Importance.HIGH, KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC)
            .define(KAFKA_STARTING_OFFSETS_KEY, Type.STRING, Importance.HIGH, KAFKA_STARTING_OFFSETS_DOC)

    def apply(): BPSConfig = new BPSConfig(configDef, baseProps)

    def apply(props: util.Map[_, _]): BPSConfig = new BPSConfig(configDef, baseProps)

    private def baseProps: util.Map[_, _] = {
        val appConfigPath: String = sys.env.getOrElse(kafkaConfigEnvPath, defaultKafkaConfigsPath)
        val props = new Properties()
        props.load(new FileInputStream(appConfigPath))
        props
    }

}
