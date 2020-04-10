package com.pharbers.StreamEngine.Utils.Event

import java.sql.Timestamp
import org.json4s._
import org.json4s.jackson.JsonMethods._


/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/04/07 16:38
  * @note 一些值得注意的地方
  */
case class BPSTypeEvents[T](events: BPSEvents) {
//    implicit val formats = DefaultFormats.preservingEmptyValues
    val jobId: String = events.jobId
    val traceId: String = events.traceId
    val `type`: String = events.`type`
    val timestamp: Timestamp = events.timestamp

    def date(implicit fmt: Formats = DefaultFormats, mf: Manifest[T]): T =
        Extraction.extract(parse(events.data))
}
