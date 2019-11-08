package com.pharbers.StreamEngine.Utils

package object Component {
    case class ComponentConfig(id: String, name: String, args: List[String], config: Map[String, String])

    //todo: 还不会用scala注解
    //class Component(val name: String, val `type`: String, val factory: Option[String]) extends annotation.StaticAnnotation

    class Factory(val name: String, val `type`: String) extends annotation.StaticAnnotation
}
