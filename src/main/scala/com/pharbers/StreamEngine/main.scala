
import java.io._

import com.pharbers.StreamEngine.Utils.Component.ComponentContext
import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor
import org.apache.logging.log4j.core.config.ConfigurationSource
import org.apache.logging.log4j.core.config.Configurator
import java.io.FileInputStream

import com.pharbers.StreamEngine.Utils.Config.AppConfig

object main extends App {
    Configurator.initialize(null, new ConfigurationSource(new FileInputStream(new File(AppConfig().getString(AppConfig.LOG_CONFIG_PATH_KEY)))))
    val context = ComponentContext()
    ThreadExecutor.waitForShutdown()
}


