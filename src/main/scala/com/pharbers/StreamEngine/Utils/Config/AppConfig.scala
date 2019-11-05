package com.pharbers.StreamEngine.Utils.Config

import java.util
import java.io.FileInputStream
import java.util.{Map, Properties}
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

object AppConfig  {

    final private  val CD: ConfigDef = baseConfigDef
    final private  val PROP: Map[_, _] = baseProps

    final private val APP_CONFIG_PATH_KEY = "path"
    final private val DEFAULT_APP_CONFIG_PATH = "src/main/resources/app.config.properties"

    final val PROJECT_NAME_KEY = "project.name"
    final private val PROJECT_NAME_DOC = "The name is project name."

    final val HOSTNAME_KEY = "hostname"
    final private val HOSTNAME_DOC = "The hostname is the host'name."

    final val COMPONENT_CONFIG_PATH = "component.config.path"
    final private val COMPONENT_CONFIG_PATH_DOC = "组件配置文件路径"

    final val COMPONENT_PACKAGES = "component.packages"
    final private val COMPONENT_PACKAGES_DOC = "组件包目录"

    final val THREAD_MAX_KEY = "thread.max"
    final private val THREAD_MAX_DOC = "线程池最大线程数"
//    final val JOBS = "jobs"
//    final private val JOBS_DOC = "随项目一起启动的job"

    private val ac = new AppConfig(CD, PROP)
    def apply(): AppConfig = ac

    def apply(props: Map[_, _]): AppConfig = new AppConfig(CD, props)

    private def baseConfigDef: ConfigDef = {
        new ConfigDef()
            .define(
                PROJECT_NAME_KEY,
                Type.STRING,
                Importance.HIGH,
                PROJECT_NAME_DOC)
            .define(
                HOSTNAME_KEY,
                Type.STRING,
                Importance.HIGH,
                HOSTNAME_DOC
            )
            .define(
                COMPONENT_CONFIG_PATH,
                Type.STRING,
                Importance.HIGH,
                COMPONENT_CONFIG_PATH_DOC
            )
            .define(
                COMPONENT_PACKAGES,
                Type.LIST,
                Importance.HIGH,
                COMPONENT_PACKAGES_DOC
            )
            .define(
                THREAD_MAX_KEY,
                Type.INT,
                Importance.HIGH,
                THREAD_MAX_DOC
            )
    }

    private def baseProps: Map[_, _] = {
        val appConfigPath: String = sys.env.getOrElse(APP_CONFIG_PATH_KEY, DEFAULT_APP_CONFIG_PATH)
        val props = new Properties()
        props.load(new FileInputStream(appConfigPath))
        props
    }

}

class AppConfig(definition: ConfigDef, originals: util.Map[_, _]) extends AbstractConfig(definition, originals)
