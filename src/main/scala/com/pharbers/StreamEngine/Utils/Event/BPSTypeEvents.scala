package com.pharbers.StreamEngine.Utils.Event

import java.sql.Timestamp
import org.json4s._
import org.json4s.jackson.JsonMethods._


/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数 case class，scala集合类，scala基本类
  * @author dcs
  * @version 0.0
  * @since 2020/04/07 16:38
  * @note 如果在泛型方法中使用，需要定义泛型时继承Manifest
  */
class BPSTypeEvents[T](events: BPSEvents) {
//    implicit val formats = DefaultFormats.preservingEmptyValues
    val jobId: String = events.jobId
    val traceId: String = events.traceId
    val `type`: String = events.`type`
    val timestamp: Timestamp = events.timestamp

    def data(implicit fmt: Formats = DefaultFormats, mf: Manifest[T]): T =
        Extraction.extract(parse(events.data))
}

object BPSTypeEvents {
    def apply[T](events: BPSEvents): BPSTypeEvents[T] = new BPSTypeEvents(events)

    def apply[T](jobId: String, traceId: String, `type`: String, data: String, timestamp: Timestamp = new Timestamp(0)): BPSTypeEvents[T] =
        new BPSTypeEvents(BPSEvents(jobId, traceId, `type`, data, timestamp))

}
