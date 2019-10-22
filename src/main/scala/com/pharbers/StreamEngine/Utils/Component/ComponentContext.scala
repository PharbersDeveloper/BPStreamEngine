package com.pharbers.StreamEngine.Utils.Component

import com.pharbers.StreamEngine.Utils.Config.AppConfig
import org.json4s._
import org.json4s.jackson.Serialization.read

import scala.io.Source

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/10/16 11:07
  * @note 一些值得注意的地方
  */
object ComponentContext{
    //以后可以通过配置选择需要的实现
    private val instance: ComponentContext = new BaseComponentContext(getConfigs(AppConfig().getString(AppConfig.COMPONENT_CONFIG_PATH)))
    def apply(): ComponentContext = instance
    def getConfigs(path: String): List[ComponentConfig] = {
        implicit val formats: DefaultFormats.type = DefaultFormats
        read[List[ComponentConfig]](Source.fromFile(path, "UTF-8").mkString)
    }
}

trait ComponentContext{

    def buildComponent[T <: AnyRef](config: ComponentConfig): T

    def buildComponent[T <: AnyRef](id: String, name: String, args: List[AnyRef], config: Map[String, String]): T

    def getComponent[T <: AnyRef](id: String): T

    def createComponent[T <: AnyRef](id: String, needAppend: Boolean = false): T
}

case class ComponentConfig(id: String, name: String, args: List[String], config: Map[String, String])

//todo: 还不会用scala注解
//class Component(val name: String, val `type`: String, val factory: Option[String]) extends annotation.StaticAnnotation

class Factory(val name: String, val `type`: String) extends annotation.StaticAnnotation