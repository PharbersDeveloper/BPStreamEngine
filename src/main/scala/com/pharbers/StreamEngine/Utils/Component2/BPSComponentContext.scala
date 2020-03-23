package com.pharbers.StreamEngine.Utils.Component2

import com.pharbers.StreamEngine.Utils.Annotation.{AnnotationSelector, Component}
import com.pharbers.StreamEngine.Utils.Config.AppConfig

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe

/** 功能描述
 *
 * @author dcs
 * @version 0.0
 * @since 2019/10/16 16:01
 * @note 一些值得注意的地方
 */
class BPSComponentContext {
    val componentAnnotations: Map[String, (String, Component)] = AppConfig().getList(AppConfig.COMPONENT_PACKAGES_KEY).asScala
            .flatMap(x => AnnotationSelector.getAnnotationClass(x, classOf[Component], true))
            .map(x => (x._2.name, (x._1, x._2))).toMap

    def buildComponent[T](config: BPConfig): T = {
        val args = config.args
        //todo: 检查是否是可用的factory
        // TODO: 统一Factory, 这里要避免递归构造
        val factory = componentAnnotations(config.name)._2.factory match {
            case "default" => componentAnnotations(config.name)._1
            case s => s
        }
        val classMirror = universe.runtimeMirror(getClass.getClassLoader)
        val factoryClass = classMirror.staticModule(factory)
        val methods = classMirror.reflectModule(factoryClass)
        val objMirror = classMirror.reflect(methods.instance)
        val method = methods.symbol.typeSignature.member(universe.TermName("apply")).asTerm.alternatives
                //todo: 根据type来确定方法，而不是参数数量
                .find(x => x.asMethod.paramLists.map(_.map(_.typeSignature)).head.length == 1).get.asMethod
//                .find(x => x.asMethod.paramLists.map(_.map(_.typeSignature)).head.length == args.length + 1).get.asMethod
        val component = objMirror.reflectMethod(method)(config.config)
        print(component.asInstanceOf[BPComponent])
        component.asInstanceOf[T]
    }
}
