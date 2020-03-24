package com.pharbers.StreamEngine.Jobs.Hive2EsJob

import com.pharbers.StreamEngine.Jobs.Hive2EsJob.strategy.{BPSMaxDataHive2EsStrategy, BPSStrategy}
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Config.BPSConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSJobStrategy
import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}

object BPSHive2EsJob {

    final val SQL_STRING_KEY = "sql"
    final val SQL_STRING_DOC = "The value is a sql string for search hive table."

    final val INDEX_NAME_KEY = "index"
    final val INDEX_NAME_DOC = "The value is a index used for elasticsearch."

    final val STRATEGY_CMD_KEY = "strategy"
    final val STRATEGY_CMD_DEFAULT = ""
    final val STRATEGY_CMD_MaxDashboard = "max_dashboard"
    final val STRATEGY_CMD_DOC = "The value is a strategy used for this job."

    final val CHECKPOINT_LOCATION_KEY = "checkpoint.location"
    final val CHECKPOINT_LOCATION_DOC = "The value is a checkpoint location which this spark stream job used."

    private val configDef: ConfigDef = new ConfigDef()
        .define(SQL_STRING_KEY, Type.STRING, Importance.HIGH, SQL_STRING_DOC)
        .define(INDEX_NAME_KEY, Type.STRING, Importance.HIGH, INDEX_NAME_DOC)
        .define(STRATEGY_CMD_KEY, Type.STRING, STRATEGY_CMD_DEFAULT, Importance.HIGH, STRATEGY_CMD_DOC)
        .define(CHECKPOINT_LOCATION_KEY, Type.STRING, Importance.HIGH, CHECKPOINT_LOCATION_DOC)

    def apply(id: String,
              spark: SparkSession,
              container: BPSJobContainer,
              jobConf: Map[String, String]): BPSHive2EsJob =
        new BPSHive2EsJob(id, spark, container, jobConf)
}

/** 执行 Hive2Es 的 Job
  *
  * @author jeorch
  * @version 0.1
  * @since 2019/12/16 15:43
  */
class BPSHive2EsJob(override val id: String,
                    override val spark: SparkSession,
                    container: BPSJobContainer,
                    jobConf: Map[String, String])
        extends BPStreamJob {

    //TODO:这Strategy太死板，无法重用
    type T = BPSJobStrategy
    override val strategy: BPSJobStrategy = null

    //TODO:InnerJobStrategy只需实现convert函数，来处理包装job内数据，输入输出的数据格式一致
    type InnerJobDataType = DataFrame
    var InnerJobStrategy: BPSStrategy[InnerJobDataType] = null

    import BPSHive2EsJob._
    private val jobConfig: BPSConfig = BPSConfig(configDef, jobConf)
    val sqlString: String = jobConfig.getString(SQL_STRING_KEY)
    val indexName: String = jobConfig.getString(INDEX_NAME_KEY)
    val strategyCMD: String = jobConfig.getString(STRATEGY_CMD_KEY)
    val checkpointLocation: String = jobConfig.getString(CHECKPOINT_LOCATION_KEY)

    override def open(): Unit = {
        logger.info("hive to es job start with id ========>" + id)
        container.jobs += id -> this
        val reading = spark.sql(sqlString)
        inputStream = Some(reading)

        //根据不同策略指令来选用策略函数处理job内部数据，默认空指令则不处理
        strategyCMD match {
            case STRATEGY_CMD_MaxDashboard => InnerJobStrategy = BPSMaxDataHive2EsStrategy(spark)
        }

    }

    override def exec(): Unit = {

        inputStream match {
            case Some(df) =>
                val length = df.count()
                logger.info("hive to es job length =  ========>" + length)
                if(length != 0){

                    //根据不同策略指令来选用策略函数处理job内部数据，默认空指令则不处理
                    val newDF: InnerJobDataType = if (InnerJobStrategy != null) { InnerJobStrategy.convert(df) } else df

                    newDF.write
                        .option("checkpointLocation", checkpointLocation)
                        .format("es")
                        .save(indexName)
                }

            case None => ???
        }
        //TODO:这里暂时是批处理，写入结束后直接关闭本job
        close()
    }

    override def close(): Unit = {
        container.finishJobWithId(id)
        logger.info("hive to es job closed with id ========>" + id)
    }

    override val componentProperty: Component2.BPComponentConfig = null

    override def createConfigDef(): ConfigDef = ???
}
