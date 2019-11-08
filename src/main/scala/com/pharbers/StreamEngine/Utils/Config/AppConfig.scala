package com.pharbers.StreamEngine.Utils.Config

import java.util
import java.util.Properties
import java.io.FileInputStream
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}

object AppConfig {
    val appConfigEnvPath: String = "path"
    val defaultAppConfigsPath: String = "src/main/resources/app.config.properties"

    final val PROJECT_NAME_KEY = "project.name"
    final val PROJECT_NAME_DOC = "The name is project name."

    final val HOSTNAME_KEY = "hostname"
    final val HOSTNAME_DOC = "The hostname is the host'name."

    final val COMPONENT_CONFIG_PATH_KEY = "component.config.path"
    final val COMPONENT_CONFIG_PATH_DOC = "组件配置文件路径"

    final val COMPONENT_PACKAGES_KEY = "component.packages"
    final val COMPONENT_PACKAGES_DOC = "组件包目录"

    final val THREAD_MAX_KEY = "thread.max"
    final val THREAD_MAX_DOC = "线程池最大线程数"

    final val LOG_CONFIG_PATH_KEY = "log.config.path"
    final val  LOG_CONFIG_PATH_DOC = "log配置文件路径"

    private def configDef: ConfigDef = new ConfigDef()
                .define(PROJECT_NAME_KEY, Type.STRING, Importance.HIGH, PROJECT_NAME_DOC)
                .define(HOSTNAME_KEY, Type.STRING, Importance.HIGH, HOSTNAME_DOC)
                .define(COMPONENT_CONFIG_PATH_KEY, Type.STRING, Importance.HIGH, COMPONENT_CONFIG_PATH_DOC)
                .define(COMPONENT_PACKAGES_KEY, Type.LIST, Importance.HIGH, COMPONENT_PACKAGES_DOC)
                .define(THREAD_MAX_KEY, Type.INT, Importance.HIGH, THREAD_MAX_DOC)
                .define(LOG_CONFIG_PATH_KEY, Type.STRING, Importance.HIGH, LOG_CONFIG_PATH_DOC)

    // 保持单例
    lazy val bc: BPSConfig = BPSConfig(configDef, baseProps)
    def apply(): BPSConfig = bc

    def apply(props: Map[String, String]): BPSConfig = BPSConfig(configDef, props)

    private def baseProps: util.Map[_, _] = {
        val appConfigPath: String = sys.env.getOrElse(appConfigEnvPath, defaultAppConfigsPath)
        val props = new Properties()
        props.load(new FileInputStream(appConfigPath))
        props
    }
}
