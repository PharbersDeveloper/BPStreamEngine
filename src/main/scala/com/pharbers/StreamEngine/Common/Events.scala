package com.pharbers.StreamEngine.Common

import java.sql.Timestamp

case class Events(jobId: String, traceId: String, `type`: String, data: String, timestamp: Timestamp = new Timestamp(0))
