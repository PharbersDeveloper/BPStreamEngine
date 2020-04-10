package com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.msgMode


/** spark structured stream query事件封装
  *
  * @param id 对一个查询的唯一标识
  * @param runId 对一次运行查询的标识
  * @param status 运行状态start，progress，terminated
  * @param msg start为start，progress为具体运行情况json，terminated为异常或者normal termination
  * @author EDZ
  * @version 0.0
  * @since 2020/4/9 14:54
  * @note id是对一个查询的唯一标识，runId是对一次运行查询的标识，如果失败重启会导致id不变runId变化
  * @example {{{这是一个例子}}}
  */
case class SparkQueryEvent(id: String, runId: String, status: String, msg: String)
