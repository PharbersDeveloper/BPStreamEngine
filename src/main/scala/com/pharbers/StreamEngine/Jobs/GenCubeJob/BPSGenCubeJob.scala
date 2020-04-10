package com.pharbers.StreamEngine.Jobs.GenCubeJob

import java.util.UUID

import com.pharbers.StreamEngine.Jobs.GenCubeJob.strategy.{BPSHandleHiveResultStrategy, BPSStrategy}
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Config.BPSConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.pharbers.StreamEngine.Utils.Strategy.BPStrategyComponent
import com.pharbers.StreamEngine.Utils.Job.{BPSJobContainer, BPStreamJob}
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}

object BPSGenCubeJob {

    final val INPUT_DATA_TYPE_KEY = "InputDataType"
    final val INPUT_DATA_TYPE_DOC = "The value is input data type."
    final val HIVE_DATA_TYPE = "hive"
    final val DF_DATA_TYPE = "df"

    final val INPUT_PATH_KEY = "InputPath"
    final val INPUT_PATH_DOC = "The value is input data path."

    final val OUTPUT_DATA_TYPE_KEY = "OutputDataType"
    final val OUTPUT_DATA_TYPE_DOC = "The value is output data type."

    final val OUTPUT_PATH_KEY = "OutputPath"
    final val OUTPUT_PATH_DOC = "The value is output data path."

    final val STRATEGY_CMD_KEY = "strategy"
    final val STRATEGY_CMD_DEFAULT = ""
    final val STRATEGY_CMD_HANDLE_HIVE_RESULT = "handle-hive-result"
    final val STRATEGY_CMD_DOC = "The value is a strategy used for this job."

    final val CHECKPOINT_LOCATION_KEY = "checkpoint.location"
    final val CHECKPOINT_LOCATION_DOC = "The value is a checkpoint location which this spark stream job used."

    private val configDef: ConfigDef = new ConfigDef()
        .define(INPUT_DATA_TYPE_KEY, Type.STRING, Importance.HIGH, INPUT_DATA_TYPE_DOC)
        .define(INPUT_PATH_KEY, Type.STRING, Importance.HIGH, INPUT_PATH_DOC)
        .define(OUTPUT_DATA_TYPE_KEY, Type.STRING, Importance.HIGH, OUTPUT_DATA_TYPE_DOC)
        .define(OUTPUT_PATH_KEY, Type.STRING, Importance.HIGH, OUTPUT_PATH_DOC)
        .define(STRATEGY_CMD_KEY, Type.STRING, STRATEGY_CMD_DEFAULT, Importance.HIGH, STRATEGY_CMD_DOC)
        .define(CHECKPOINT_LOCATION_KEY, Type.STRING, Importance.HIGH, CHECKPOINT_LOCATION_DOC)

    def apply(id: String,
              spark: SparkSession,
              container: BPSJobContainer,
              jobConf: Map[String, String]): BPSGenCubeJob =
        new BPSGenCubeJob(id, spark, container, jobConf)
}

/** 执行 GenCube 的 Job
  *
  * @author jeorch
  * @version 0.0.1
  * @since 2020/3/19 15:43
  */
class BPSGenCubeJob(override val id: String,
                    override val spark: SparkSession,
                    container: BPSJobContainer,
                    jobConf: Map[String, String])
        extends BPStreamJob {

    type T = BPStrategyComponent
    override val strategy: BPStrategyComponent = null

    type InnerJobDataType = DataFrame
    var InnerJobStrategy: BPSStrategy[InnerJobDataType] = null

    import BPSGenCubeJob._
    private val jobConfig: BPSConfig = BPSConfig(configDef, jobConf)
    val inputDataType: String = jobConfig.getString(INPUT_DATA_TYPE_KEY)
    val inputPath: String = jobConfig.getString(INPUT_PATH_KEY)
    val outputDataType: String = jobConfig.getString(OUTPUT_DATA_TYPE_KEY)
    val outputPath: String = jobConfig.getString(OUTPUT_PATH_KEY)
    val strategyCMD: String = jobConfig.getString(STRATEGY_CMD_KEY)
    val checkpointLocation: String = jobConfig.getString(CHECKPOINT_LOCATION_KEY)

    override def open(): Unit = {
        logger.info("gen-cube job start with id ========>" + id)
        container.jobs += id -> this
        val reading = inputDataType match {
            case HIVE_DATA_TYPE => spark.sql(inputPath)
            //TODO: 其他输入数据格式待补充
            case DF_DATA_TYPE => ???
            case _ => ???
        }
        inputStream = Some(reading)

        //根据不同策略指令来选用策略函数处理job内部数据，默认空指令则不处理
        strategyCMD match {
            case STRATEGY_CMD_HANDLE_HIVE_RESULT => InnerJobStrategy = BPSHandleHiveResultStrategy(spark)
        }

    }

    override def exec(): Unit = {

        inputStream match {
            case Some(df) =>
                val length = df.count()
                logger.info("gen-cube job length =  ========>" + length)
                if(length != 0){

                    //根据不同策略指令来选用策略函数处理job内部数据，默认空指令则不处理
                    val newDF: InnerJobDataType = if (InnerJobStrategy != null) { InnerJobStrategy.convert(df) } else df

                    val uuid = UUID.randomUUID().toString
                    val savePath = outputPath + s"/${uuid}"
                    newDF
//                        .coalesce(1)
                        .write
                        .option("checkpointLocation", checkpointLocation)
                        .format(outputDataType)
                        .option("header", value = true)
                        .save(savePath)
                    logger.info(s"Succeed save cube in path=${savePath}.")
                }

            case None => ???
        }
        //TODO:这里暂时是批处理，写入结束后直接关闭本job
        close()
    }

    override def close(): Unit = {
        container.finishJobWithId(id)
        logger.info("gen-cube job closed with id ========>" + id)
    }

    override val description: String = "cube gen"
    override val componentProperty: Component2.BPComponentConfig = null
    override def createConfigDef(): ConfigDef = ???
}
