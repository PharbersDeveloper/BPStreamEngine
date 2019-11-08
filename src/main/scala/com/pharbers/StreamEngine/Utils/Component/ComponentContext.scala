package com.pharbers.StreamEngine.Utils.Component

import org.json4s._
import scala.io.Source
import org.json4s.jackson.Serialization.read
import com.pharbers.StreamEngine.Utils.Config.AppConfig

/** 功能描述
 *
 * @author dcs
 * @version 0.0
 * @since 2019/10/16 11:07
 * @note 一些值得注意的地方
 */
object ComponentContext {
    //TODO 以后可以通过配置选择需要的实现
    def init(): ComponentContext = new BaseComponentContext(
        getComponentConfigLst(AppConfig().getString(AppConfig.COMPONENT_CONFIG_PATH_KEY))
    )

    private def getComponentConfigLst(path: String): List[ComponentConfig] = {
        implicit val formats: DefaultFormats.type = DefaultFormats
        val bs = Source.fromFile(path, "UTF-8")
        val content = bs.mkString
        read[List[ComponentConfig]](content)
    }
}

trait ComponentContext {
    def buildComponent[T <: AnyRef](config: ComponentConfig): T

    def buildComponent[T <: AnyRef](id: String, name: String, args: List[AnyRef], config: Map[String, String]): T

    def getComponent[T <: AnyRef](id: String): T

    def createComponent[T <: AnyRef](id: String, needAppend: Boolean = false): T
}
