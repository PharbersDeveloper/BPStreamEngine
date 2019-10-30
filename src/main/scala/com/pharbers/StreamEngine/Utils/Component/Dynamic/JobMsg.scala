package com.pharbers.StreamEngine.Utils.Component.Dynamic

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/10/22 15:33
  * @note 一些值得注意的地方
  */
case class JobMsg(id: String, `type`: String, classPath: String, args: List[String], dependencies: List[String], dependencyArgs: List[String],
                  config: Map[String, String], dependencyStop: String, description: String)
