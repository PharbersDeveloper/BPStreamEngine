package com.pharbers.StreamEngine.Utils.Log

import java.io.{File, FileInputStream}
import com.pharbers.StreamEngine.Utils.Config.AppConfig
import org.apache.logging.log4j.core.config.{ConfigurationSource, Configurator}

/** 启动 LogContext
 *
 * @author clock
 * @version 0.1
 * @since 2019/11/08 14:55
 */
object BPSLogContext {
    def init(): Unit = Configurator.initialize(null,
        new ConfigurationSource(
            new FileInputStream(
                new File(AppConfig().getString(AppConfig.LOG_CONFIG_PATH_KEY))
            )
        )
    )
}
