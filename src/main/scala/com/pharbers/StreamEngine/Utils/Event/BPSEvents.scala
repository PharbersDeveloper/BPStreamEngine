package com.pharbers.StreamEngine.Utils.Event

import java.sql.Timestamp
import org.json4s._
import org.json4s.jackson.Serialization.write

case class BPSEvents(jobId: String, traceId: String, `type`: String, data: String, timestamp: Timestamp = new Timestamp(0))

object BPSEvents {
    def apply(jobId: String, traceId: String, `type`: String, data: AnyRef): BPSEvents = {
        implicit val formats: DefaultFormats.type = DefaultFormats
        new BPSEvents(jobId, traceId, `type`, write(data))
    }

    def apply(jobId: String, traceId: String, `type`: String, data: String): BPSEvents = {
        implicit val formats: DefaultFormats.type = DefaultFormats
        new BPSEvents(jobId, traceId, `type`, data)
    }
}