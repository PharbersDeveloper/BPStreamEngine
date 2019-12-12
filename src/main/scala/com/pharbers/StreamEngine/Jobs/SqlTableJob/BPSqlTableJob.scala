package com.pharbers.StreamEngine.Jobs.SqlTableJob

import java.util.UUID

import com.pharbers.StreamEngine.Utils.Config.BPSConfig
import com.pharbers.StreamEngine.Utils.Schema.Spark.BPSParseSchema
import BPSqlTableJob._
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSJobStrategy
import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel


/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/12/11 14:16
  * @note 一些值得注意的地方
  */
case class BPSqlTableJob(jobContainer: BPSJobContainer, spark: SparkSession, config: Map[String, String]) extends BPStreamJob {
    override type T = BPSJobStrategy
    override val strategy: BPSJobStrategy = null
    private val jobConfig: BPSConfig = BPSConfig(configDef, config)
    val jobId: String = jobConfig.getString(JOB_ID_CONFIG_KEY)
    val runId: String = jobConfig.getString(RUN_ID_CONFIG_KEY)
    override val id: String = jobId

    val url: String = jobConfig.getString(URL_CONFIG_KEY)
    val saveMode: String = jobConfig.getString(TASK_TYPE_CONFIG_KEY)
    val metadataPath: String = jobConfig.getString(METADATA_PATH_CONFIG_KEY)


    override def open(): Unit = {
        inputStream = Some(spark.read
                .format("csv")
                .option("header", true)
                .option("delimiter", ",")
                .load(url)
//todo: 先判断有没有YEAR和MONTH
//                .repartition(col("YEAR"), col("MONTH"))
                .persist(StorageLevel.MEMORY_ONLY)
        )
    }

    override def exec(): Unit = {
        val metadata = BPSParseSchema.parseMetadata(metadataPath)(spark)
        val providers = metadata.getOrElse("providers", List("")).asInstanceOf[List[String]]
        if(providers.contains("CPA&GYC")) {
            val tableName = "cpa"
            logger.info(s"start save table $tableName, mode: $saveMode")
            saveMode match {
                case "append" => appendTable(tableName)
                //todo: 全量数据处理
                case _ => ???
            }
            logger.info(s"save $tableName over, close job $id")
        }
        close()
    }

    override def close(): Unit = {
        super.close()
        jobContainer.finishJobWithId(id)
    }

    def appendTable(tableName: String): Unit = {
        //todo: 需要检查已经有的
        val version = "0.0.0"
        inputStream match {
            case Some(df) =>
                val count = df.count()
                logger.info(s"url: $url, count: $count")
//                if(count != 0){
//                    df.withColumn("version", lit(version)).write
//                            .partitionBy("YEAR", "MONTH")
//                            .mode(saveMode)
//                            .option("path", s"/common/public/$tableName/$version")
//                            .saveAsTable(tableName)
//                }
            case _ =>
        }
    }

}

object BPSqlTableJob {
    final val JOB_ID_CONFIG_KEY = "jobId"
    final val JOB_ID_CONFIG_DOC = "job id"
    final val RUN_ID_CONFIG_KEY = "runId"
    final val RUN_ID_CONFIG_DOC = "run id"
    final val URL_CONFIG_KEY = "url"
    final val URL_CONFIG_DOC = "content path"
    final val METADATA_PATH_CONFIG_KEY = "metadataPath"
    final val METADATA_PATH_CONFIG_DOC = "metadataPath"
    final val TASK_TYPE_CONFIG_KEY = "taskType"
    final val TASK_TYPE_CONFIG_DOC = "append or overwrite"
    val configDef: ConfigDef = new ConfigDef()
            .define(JOB_ID_CONFIG_KEY, Type.STRING, UUID.randomUUID().toString, Importance.HIGH, JOB_ID_CONFIG_DOC)
            .define(RUN_ID_CONFIG_KEY, Type.STRING, UUID.randomUUID().toString, Importance.HIGH, RUN_ID_CONFIG_DOC)
            .define(URL_CONFIG_KEY, Type.STRING, "", Importance.HIGH, URL_CONFIG_DOC)
            .define(METADATA_PATH_CONFIG_KEY, Type.STRING, "", Importance.HIGH, METADATA_PATH_CONFIG_DOC)
            .define(TASK_TYPE_CONFIG_KEY, Type.STRING, "append", Importance.HIGH, TASK_TYPE_CONFIG_DOC)

}
