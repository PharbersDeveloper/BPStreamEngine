package com.pharbers.StreamEngine.Jobs.SqlTableJob

import com.pharbers.StreamEngine.Utils.Config.BPSConfig
import BPSqlTableJob._
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Job.{BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Utils.Strategy.BPSDataMartBaseStrategy
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
case class BPSqlTableJob(jobContainer: BPSJobContainer, spark: SparkSession, config_map: Map[String, String]) extends BPStreamJob {
    override val componentProperty: Component2.BPComponentConfig = null
    override def createConfigDef(): ConfigDef = new ConfigDef()
            .define(URLS_CONFIG_KEY, Type.LIST, "", Importance.HIGH, URLS_CONFIG_DOC)
            .define(TABLE_NAME_CONFIG_KEY, Type.STRING, "", Importance.HIGH, TABLE_NAME_CONFIG_DOC)
            .define(TASK_TYPE_CONFIG_KEY, Type.STRING, "append", Importance.HIGH, TASK_TYPE_CONFIG_DOC)
            .define(ERROR_PATH_CONFIG_KEY, Type.STRING, "", Importance.HIGH, ERROR_PATH_CONFIG_DOC)
            //            .define(VERSION_CONFIG_KEY, Type.STRING, Importance.HIGH, VERSION_CONFIG_DOC)
            .define(DATA_SETS_CONFIG_KEY, Type.LIST, "", Importance.HIGH, DATA_SETS_CONFIG_DOC)
    override type T = BPSDataMartBaseStrategy
    override val strategy: BPSDataMartBaseStrategy = new BPSDataMartBaseStrategy(config_map, configDef)
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
        val url = s"/common/public/$tableName/$version"
        logger.info(s"start save table $tableName, mode: $saveMode")
        saveMode match {
            case "append" => saveTable(tableName, saveMode, version, url)
            //todo: 全量数据处理
            case "overwrite" =>
                spark.sql(s"drop table $tableName")
                saveTable(tableName, saveMode, version, url)
            case _ => ???
        }
        logger.info(s"save $tableName over, job: $id")
        logger.info(s"push data set")
        strategy.pushDataSet(tableName, version, url, saveMode)
        logger.info(s"close job $id")
        close()
    }

    override def close(): Unit = {
        super.close()
        jobContainer.finishJobWithId(id)
    }

    def saveTable(tableName: String, mode: String, version: String, url: String): Unit = {
        //todo: 需要检查已经有的
        inputStream match {
            case Some(df) =>
                df.coalesce(4).withColumn("version", lit(version)).write
                        .mode(mode)
                        .option("path", url)
                        .saveAsTable(tableName)
            case _ =>
        }
        val errorHead = spark.sparkContext.textFile(jobConfig.getString(ERROR_PATH_CONFIG_KEY)).take(1).headOption.getOrElse("")
        if (errorHead.length > 0) logger.info(s"error path: ${jobConfig.getString(ERROR_PATH_CONFIG_KEY)} ,error: $errorHead")
    }

    override val description: String = "sql_table"
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
}

