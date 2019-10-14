package com.pharbers.StreamEngine.Utils.Event

import java.sql.Timestamp

case class BPSEvents(jobId: String, traceId: String, `type`: String, data: String, timestamp: Timestamp = new Timestamp(0))
