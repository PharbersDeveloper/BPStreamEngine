package com.pharbers.StreamEngine.Utils.Strategy.Log

import java.io.{File, FileInputStream}

import com.pharbers.StreamEngine.Utils.Annotation.Component
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Config.AppConfig
import com.pharbers.StreamEngine.Utils.Strategy.BPStrategyComponent
import org.apache.kafka.common.config.ConfigDef
import org.apache.logging.log4j.core.config.{ConfigurationSource, Configurator}

/** 启动 LogContext
 *
 * @author clock
 * @version 0.1
 * @since 2019/11/08 14:55
 */
@Component(name = "BPSLogContext", `type` = "BPSLogContext")
case class BPSLogContext(override val componentProperty: Component2.BPComponentConfig)
    extends BPStrategyComponent {

   Configurator.initialize(null,
        new ConfigurationSource(
            new FileInputStream(
                new File(AppConfig().getString(AppConfig.LOG_CONFIG_PATH_KEY))
            )
        )
    )

    override val strategyName: String = "log4j"
    override def createConfigDef(): ConfigDef = ???
}
