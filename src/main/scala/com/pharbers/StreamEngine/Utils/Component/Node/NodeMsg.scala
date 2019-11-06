package com.pharbers.StreamEngine.Utils.Component.Node

/** 功能描述
 *
 * @author dcs
 * @version 0.0
 * @since 2019/10/22 15:12
 * @note 一些值得注意的地方
 */
case class NodeMsg(id: String, `type`: String, classPath: String, args: List[String], listeners: List[(String, Long)], handlers: List[(String, Long)],
                   jobs: List[(String, Long)], dependencies: List[(String, Long)], dependencyArgs: List[String], config: Map[String, String], dependencyStop: String,
                   description: String, timestamp: Long)
