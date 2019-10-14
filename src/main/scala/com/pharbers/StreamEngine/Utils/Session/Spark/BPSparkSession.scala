package com.pharbers.StreamEngine.Utils.Session.Spark

import java.net.InetAddress

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object BPSparkSession {
    def apply(): SparkSession = new BPSparkSession().spark
}

class BPSparkSession {
    val yarnJars: String = "hdfs://spark.master:9000/jars/sparkJars"

    private val conf = new SparkConf()
        .set("spark.yarn.jars", yarnJars)
        .set("spark.yarn.archive", yarnJars)
        .setAppName("bp-stream-engine")
        .setMaster("yarn")
        .set("spark.driver.memory", "2g")
        .set("spark.driver.cores", "2")
        .set("spark.scheduler.mode", "FAIR")
        .set("spark.executor.memory", "2g")
        .set("spark.executor.cores", "2")
        .set("spark.executor.instances", "2")

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
}
