package com.pharbers.StreamEngine.Jobs.SqlTableJob.SqlTableListener

import com.pharbers.StreamEngine.Jobs.SqlTableJob.BPSqlTableJob
import com.pharbers.StreamEngine.Jobs.SqlTableJob.SqlTableJobContainer.BPSqlTableJobContainer
import com.pharbers.StreamEngine.Utils.Channel.Local.BPSLocalChannel
import com.pharbers.StreamEngine.Utils.Config.BPSConfig
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamListener
import com.pharbers.StreamEngine.Utils.HDFS.BPSHDFSFile
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.spark.sql.DataFrame

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/12/11 14:45
  * @note 一些值得注意的地方
  */
case class BPStreamOverListener(job: BPSqlTableJobContainer, config: Map[String, String]) extends BPStreamListener{
    final val LENGTH_CONFIG_KEY = "length"
    final val LENGTH_CONFIG_DOC = "end length"
    final val ROW_RECORD_PATH_CONFIG_KEY = "rowRecordPath"
    final val ROW_RECORD_PATH_CONFIG_DOC = "already read row record path"
    val configDef: ConfigDef = new ConfigDef()
            .define(LENGTH_CONFIG_KEY, Type.LONG, 0L, Importance.HIGH, LENGTH_CONFIG_DOC)
            .define(ROW_RECORD_PATH_CONFIG_KEY, Type.STRING, "", Importance.HIGH, ROW_RECORD_PATH_CONFIG_DOC)
    private val listenerConfig: BPSConfig = BPSConfig(configDef, config)

    override def trigger(e: BPSEvents): Unit = {
        val rows = BPSHDFSFile.readHDFS(listenerConfig.getString(ROW_RECORD_PATH_CONFIG_KEY)).map(_.toLong).sum
        logger.debug(s"row record path: ${listenerConfig.getString(ROW_RECORD_PATH_CONFIG_KEY)}")
        logger.debug(s"rows: $rows")
        logger.debug(s"length: ${listenerConfig.getLong(LENGTH_CONFIG_KEY)}")
        if (rows >= listenerConfig.getLong(LENGTH_CONFIG_KEY)) {
            logger.info(s"启动sql job")
            val sqlJob = BPSqlTableJob(job, job.spark, config)
            job.addJob2Container(sqlJob)
            deActive()
        } else {
            //todo: test用
            logger.error(s"row record path: ${listenerConfig.getString(ROW_RECORD_PATH_CONFIG_KEY)}")
            logger.error(s"rows: $rows")
            logger.error(s"length: ${listenerConfig.getLong(LENGTH_CONFIG_KEY)}")
            deActive()
        }
    }

    override def active(s: DataFrame): Unit = BPSLocalChannel.registerListener(this)

    override def deActive(): Unit = BPSLocalChannel.unRegisterListener(this)
}
