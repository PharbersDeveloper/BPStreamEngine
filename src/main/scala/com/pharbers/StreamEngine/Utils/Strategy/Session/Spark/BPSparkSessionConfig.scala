package com.pharbers.StreamEngine.Utils.Strategy.Session.Spark

import com.pharbers.StreamEngine.Utils.Component2.BPComponent
import com.pharbers.StreamEngine.Utils.Strategy.Session.SessionStrategy
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}

trait BPSparkSessionConfig extends SessionStrategy {
    val defaultSparkConfigsPath = "src/main/resources/sparkConfig.properties"
    val defaultAppName = "bp-stream-engine"
    val defaultMaster = "yarn"
    val defaultLogLevel = "WARN"
    val defaultRunModel = "client"

    final val SPARK_CONFIGS_PATH_KEY = "spark.configs.path"
    final val SPARK_CONFIGS_PATH_DOC = "spark 配置文件目录"
    final val APP_NAME_KEY = "app.name"
    final val APP_NAME_DOC = "项目名称"
    final val MASTER_KEY = "master"
    final val MASTER_DOC = "master节点"
    final val LOG_LEVEL_KEY = "log.level"
    final val LOG_LEVEL_DOC = "日志等级 ERROR、WARN、INFO、DEBUG、TRACE"
    final val RUN_MODEL_KEY = "run.model"
    final val RUN_MODEL_DOC = "运行的模式client, 或者cluster"

    override def createConfigDef(): ConfigDef = new ConfigDef()
            .define(SPARK_CONFIGS_PATH_KEY, Type.STRING, defaultSparkConfigsPath, Importance.HIGH, SPARK_CONFIGS_PATH_DOC)
            .define(APP_NAME_KEY, Type.STRING, defaultAppName, Importance.HIGH, APP_NAME_DOC)
            .define(MASTER_KEY, Type.STRING, defaultMaster, Importance.HIGH, MASTER_DOC)
            .define(LOG_LEVEL_KEY, Type.STRING, defaultLogLevel, Importance.HIGH, LOG_LEVEL_DOC)
            .define(RUN_MODEL_KEY, Type.STRING, defaultRunModel, Importance.HIGH, RUN_MODEL_DOC)
}
