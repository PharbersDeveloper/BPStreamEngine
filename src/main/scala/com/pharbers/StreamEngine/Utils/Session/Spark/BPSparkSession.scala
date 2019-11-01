package com.pharbers.StreamEngine.Utils.Session.Spark

import java.io.FileInputStream
import java.net.InetAddress
import java.util.Properties

import com.pharbers.StreamEngine.Utils.Annotation.Component
import com.pharbers.StreamEngine.Utils.Config.AppConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import collection.JavaConverters._

object BPSparkSession {
    def apply(): SparkSession = new BPSparkSession(Map.empty).spark

    def apply(config: Map[String, String]): SparkSession = new BPSparkSession(config).spark
}

@Component(name = "BPSparkSession", `type` = "session")
class BPSparkSession(config: Map[String, String]) {
    val yarnJars: String = "hdfs://spark.master:9000/jars/sparkJars"
    final val SPARK_CONFIGS_PATH_KEY = "spark.configs.path"
    final val SPARK_CONFIGS_PATH_DOC = "spark 配置文件目录"
    final val APP_NAME_KEY = "app.name"
    final val APP_NAME_DOC = "项目名称"
    final val master_KEY = "master"
    final val master_DOC = "master节点"
    val configDef = new ConfigDef()
            .define(SPARK_CONFIGS_PATH_KEY, Type.STRING, null, Importance.HIGH, SPARK_CONFIGS_PATH_DOC)
            .define(APP_NAME_KEY, Type.STRING, "bp-stream-engine", Importance.HIGH, APP_NAME_DOC)
            .define(master_KEY, Type.STRING, "yarn", Importance.HIGH, master_DOC)
    val sparkConfigs = new AppConfig(configDef, config.asJava)
    //这儿的set的参数是第一优先级的
    val pops = new Properties()
//    pops.load(new FileInputStream(sparkConfigs.getString(SPARK_CONFIGS_PATH_KEY)))
    pops.load(new FileInputStream("src/main/resources/sparkConfig.properties"))
    private val conf = new SparkConf()
        .setAll(pops.asScala)
        .setAppName("bp-stream-engine")
        .setMaster("yarn")


    val spark = SparkSession.builder()
        .config(conf).getOrCreate()
    spark.sparkContext.setLocalProperty("host", InetAddress.getLocalHost.getHostAddress)
    spark.sparkContext.addFile("./kafka.broker1.keystore.jks")
    spark.sparkContext.addFile("./kafka.broker1.truststore.jks")
    spark.sparkContext.addJar("./target/BP-Stream-Engine-1.0-SNAPSHOT.jar")
    spark.sparkContext.addJar("./jars/kafka-schema-registry-client-5.2.1.jar")
    spark.sparkContext.addJar("./jars/kafka-avro-serializer-5.2.1.jar")
    spark.sparkContext.addJar("./jars/common-config-5.2.1.jar")
    spark.sparkContext.addJar("./jars/common-utils-5.2.1.jar")
    spark.sparkContext.setLogLevel("ERROR")
}
