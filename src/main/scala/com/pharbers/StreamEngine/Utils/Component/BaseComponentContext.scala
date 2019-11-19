package com.pharbers.StreamEngine.Utils.Component

import collection.JavaConverters._
import scala.reflect.runtime.universe
import com.pharbers.StreamEngine.Utils.Config.AppConfig
import com.pharbers.StreamEngine.Utils.Annotation.{AnnotationSelector, Component}

/** 功能描述
 *
 * @author dcs
 * @version 0.0
 * @since 2019/10/16 16:01
 * @note 一些值得注意的地方
 */
private[Component] class BaseComponentContext(var configs: List[ComponentConfig]) extends ComponentContext {
    private var container = Map[String, AnyRef]()
    val componentAnnotations: Map[String, (String, Component)] = AppConfig().getList(AppConfig.COMPONENT_PACKAGES_KEY).asScala
            .flatMap(x => AnnotationSelector.getAnnotationClass(x, classOf[Component], true))
            .map(x => (x._2.name, (x._1, x._2))).toMap

    configs.foreach(x => buildComponent[AnyRef](x))

    override def buildComponent[T](config: ComponentConfig): T = {
        if (!configs.exists(x => x.id == config.id)) configs = config +: configs
        //暂时不做组件替换
        if (container.contains(config.id)) return getComponent(config.id)
        val args = config.args.map(x => {
            if (x.startsWith("$")) {
                getComponent[AnyRef](x.replace("$", ""))
            } else {
                x
            }
        }) :+ config.config
        //todo: 检查是否是可用的factory
        val factory = componentAnnotations(config.name)._2.factory match {
            case "default" => componentAnnotations(config.name)._1
            case s => s
        }
        //        val component = Class.forName(factory).getDeclaredMethod("apply", args.map(x => x.getClass): _*).invoke(null, args: _*)
        val classMirror = universe.runtimeMirror(getClass.getClassLoader)
        val factoryClass = classMirror.staticModule(factory)
        val methods = classMirror.reflectModule(factoryClass)
        val objMirror = classMirror.reflect(methods.instance)
        val method = methods.symbol.typeSignature.member(universe.TermName("apply")).asTerm.alternatives
                //todo: 根据type来确定方法，而不是参数数量
                .find(x => x.asMethod.paramLists.map(_.map(_.typeSignature)).head.length == args.length).get.asMethod
        val component = objMirror.reflectMethod(method)(args: _*).asInstanceOf[AnyRef]
        container = container ++ Map(config.id -> component)
        component.asInstanceOf[T]
    }

    override def getComponent[T <: AnyRef](id: String): T = {
        val component = container.getOrElse(id, createComponent(id, true))
        component match {
            case t: T => t
            //todo: 错误日志，及异常
            case _ => ???
        }
    }

    override def createComponent[T](id: String, needAppend: Boolean = false): T = {
        buildComponent(configs.find(x => x.id == id) match {
            case Some(c) => c
            //todo: 原始配置文件中组件配置不存在时，从上传的新配置文件中添加
            case None => ???
        })
    }

    override def buildComponent[T <: AnyRef](id: String, name: String, args: List[AnyRef], config: Map[String, String]): T = ???
}
