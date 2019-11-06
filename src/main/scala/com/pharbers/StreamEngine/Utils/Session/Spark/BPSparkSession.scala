package com.pharbers.StreamEngine.Utils.Session.Spark

import java.net.InetAddress
import java.util.Properties
import java.io.FileInputStream
import collection.JavaConverters._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.kafka.common.config.ConfigDef
import com.pharbers.StreamEngine.Utils.Config.AppConfig
import com.pharbers.StreamEngine.Utils.Annotation.Component
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}

object BPSparkSession {
    def apply(): SparkSession = new BPSparkSession(Map.empty).spark

    def apply(config: Map[String, String]): SparkSession = new BPSparkSession(config).spark
}

@Component(name = "BPSparkSession", `type` = "session")
class BPSparkSession(config: Map[String, String]) extends BPSparkSessionConfig {
    // 此处 Config 的配置优先级高于 Submit 时设置的配置
    private val pops = new Properties()
    private val sparkConfigs = new AppConfig(configDef, config.asJava)
    pops.load(new FileInputStream(sparkConfigs.getString(SPARK_CONFIGS_PATH_KEY)))
    private val conf = new SparkConf()
            .setAll(pops.asScala)
            .setAppName(sparkConfigs.getString(APP_NAME_KEY))
            .setMaster(sparkConfigs.getString(MASTER_KEY))

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setLogLevel(sparkConfigs.getString(LOG_LEVEL_KEY))
    spark.sparkContext.setLocalProperty("host", InetAddress.getLocalHost.getHostAddress)

    // 初始环境设置
    spark.sparkContext.addFile("./kafka.broker1.keystore.jks")
    spark.sparkContext.addFile("./kafka.broker1.truststore.jks")
    spark.sparkContext.addJar("./target/BP-Stream-Engine-1.0-SNAPSHOT.jar")
    spark.sparkContext.addJar("./jars/kafka-schema-registry-client-5.2.1.jar")
    spark.sparkContext.addJar("./jars/kafka-avro-serializer-5.2.1.jar")
    spark.sparkContext.addJar("./jars/common-config-5.2.1.jar")
    spark.sparkContext.addJar("./jars/common-utils-5.2.1.jar")
}
