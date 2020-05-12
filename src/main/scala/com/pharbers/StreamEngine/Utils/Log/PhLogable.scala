package com.pharbers.StreamEngine.Utils.Log

import org.apache.logging.log4j.{LogManager, Logger}

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/05/06 18:15
  * @note 一些值得注意的地方
  */
trait PhLogable{
    protected val logger: Logger = LogManager.getLogger(this)
}
