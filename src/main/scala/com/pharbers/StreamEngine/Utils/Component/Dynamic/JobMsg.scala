package com.pharbers.StreamEngine.Utils.Component.Dynamic

/** 功能描述
  *
  * @author dcs
  * @version 0.1
  * @since 2019/10/22 15:33
  * @note 一些值得注意的地方
  */
case class JobMsg(id: String,
                  `type`: String,
                  classPath: String,
                  args: List[String],
                  dependencies: List[String],
                  dependencyArgs: List[String],
                  config: Map[String, String],
                  dependencyStop: String,
                  description: String)
