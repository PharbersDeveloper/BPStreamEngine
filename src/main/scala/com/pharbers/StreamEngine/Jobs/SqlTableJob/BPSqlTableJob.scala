package com.pharbers.StreamEngine.Jobs.SqlTableJob

import java.util.UUID
import java.util.concurrent.TimeUnit

import com.pharbers.StreamEngine.Utils.Config.BPSConfig
import com.pharbers.StreamEngine.Utils.Schema.Spark.BPSParseSchema
import BPSqlTableJob._
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.{BPSCommonJoBStrategy, BPSJobStrategy}
import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}
import com.pharbers.kafka.producer.PharbersKafkaProducer
import com.pharbers.kafka.schema.AssetDataMart
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import collection.JavaConverters._
import scala.collection.mutable

/** 功能描述
  *
  * @author dcs
  * @version 0.0
  * @since 2019/12/11 14:16
  * @note 一些值得注意的地方
  */
case class BPSqlTableJob(jobContainer: BPSJobContainer, spark: SparkSession, config: Map[String, String]) extends BPStreamJob {
    override type T = BPSCommonJoBStrategy
    override val strategy: BPSCommonJoBStrategy = BPSCommonJoBStrategy(config, configDef)
    private val jobConfig: BPSConfig = strategy.getJobConfig
    val jobId: String = strategy.getJobId
    val runId: String = strategy.getRunId
    override val id: String = jobId

    val urls: mutable.Buffer[String] = jobConfig.getList(URLS_CONFIG_KEY).asScala
    val saveMode: String = jobConfig.getString(TASK_TYPE_CONFIG_KEY)

    override def open(): Unit = {
        logger.info(s"open job $id")
        inputStream = Some(spark.read
                .format("csv")
                .option("header", value = true)
                .option("delimiter", ",")
                .load(urls: _*)
        )
    }

    override def exec(): Unit = {
        val tableName = jobConfig.getString(TABLE_NAME_CONFIG_KEY)
        val tables = spark.sql("show tables").select("tableName").collect().map(x => x.getString(0))
        val version = if (tables.contains(tableName)) {
            val old = spark.sql(s"select version from $tableName limit 1").take(1).head.getString(0).split("\\.")
            saveMode match {
                case "append" => old.mkString(".")
                case "overwrite" => s"${old.head}.${old(1)}.${old(2).toInt + 1}"
                case _ => old.mkString(".")
            }
        } else {
            "0.0.1"
        }
        logger.info(s"start save table $tableName, mode: $saveMode")
        saveMode match {
            case "append" => saveTable(tableName, saveMode, version)
            //todo: 全量数据处理
            case "overwrite" =>
                spark.sql(s"drop table $tableName")
                saveTable(tableName, saveMode, version)
            case _ => ???
        }
        logger.info(s"save $tableName over, job: $id")
        val producer = new PharbersKafkaProducer[String, AssetDataMart]()
        val value = new AssetDataMart(
            tableName,
            "",
            version,
            "mart",
            List[CharSequence]("*").asJava,
            List[CharSequence]("*").asJava,
            List[CharSequence]("*").asJava,
            List[CharSequence]("*").asJava,
            List[CharSequence]("*").asJava,
            List[CharSequence]("*").asJava,
            List[CharSequence](jobConfig.getList(DATA_SETS_CONFIG_KEY).asScala: _*).asJava,
            tableName,
            s"/common/public/$tableName/$version",
            "hive",
            saveMode
        )
        val fu = producer.produce("AssetDataMart", "", value)
        logger.info(fu.get(10, TimeUnit.SECONDS))
        logger.info(s"close job $id")
        close()
    }

    override def close(): Unit = {
        super.close()
        jobContainer.finishJobWithId(id)
    }

    def saveTable(tableName: String, mode: String, version: String): Unit = {
        //todo: 需要检查已经有的
        inputStream match {
            case Some(df) =>
                df.coalesce(4).withColumn("version", lit(version)).write
                        .mode(mode)
                        .option("path", s"/common/public/$tableName/$version")
                        .saveAsTable(tableName)
            case _ =>
        }
        val errorHead = spark.sparkContext.textFile(jobConfig.getString(ERROR_PATH_CONFIG_KEY)).take(1).headOption.getOrElse("")
        if (errorHead.length > 0) logger.info(s"error path: ${jobConfig.getString(ERROR_PATH_CONFIG_KEY)} ,error: $errorHead")
    }
}

object BPSqlTableJob {
    final val URLS_CONFIG_KEY = "urls"
    final val URLS_CONFIG_DOC = "content paths one or many"
    final val TABLE_NAME_CONFIG_KEY = "tableName"
    final val TABLE_NAME_CONFIG_DOC = "table name"
    final val TASK_TYPE_CONFIG_KEY = "taskType"
    final val TASK_TYPE_CONFIG_DOC = "append or overwrite"
    final val ERROR_PATH_CONFIG_KEY = "errorPath"
    final val ERROR_PATH_CONFIG_DOC = "error row  path"
    final val DATA_SETS_CONFIG_KEY = "dataSets"
    final val DATA_SETS_CONFIG_DOC = "dataSet ids"
    //    final val VERSION_CONFIG_KEY = "version"
    //    final val VERSION_CONFIG_DOC = "version in asset"
    val configDef: ConfigDef = new ConfigDef()
            .define(URLS_CONFIG_KEY, Type.LIST, "", Importance.HIGH, URLS_CONFIG_DOC)
            .define(TABLE_NAME_CONFIG_KEY, Type.STRING, "", Importance.HIGH, TABLE_NAME_CONFIG_DOC)
            .define(TASK_TYPE_CONFIG_KEY, Type.STRING, "append", Importance.HIGH, TASK_TYPE_CONFIG_DOC)
            .define(ERROR_PATH_CONFIG_KEY, Type.STRING, "", Importance.HIGH, ERROR_PATH_CONFIG_DOC)
            //            .define(VERSION_CONFIG_KEY, Type.STRING, Importance.HIGH, VERSION_CONFIG_DOC)
            .define(DATA_SETS_CONFIG_KEY, Type.LIST, "", Importance.HIGH, DATA_SETS_CONFIG_DOC)

}

