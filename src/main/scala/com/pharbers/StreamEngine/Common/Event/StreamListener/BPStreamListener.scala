package com.pharbers.StreamEngine.Common.Event.StreamListener

import com.pharbers.StreamEngine.BPStreamJob.BPStreamJob
import com.pharbers.StreamEngine.Common.Event.BPSEvents
import org.apache.spark.sql

trait BPStreamListener {
    val job: BPStreamJob

    def trigger(e: BPSEvents): Unit
    def hit(e: BPSEvents): Boolean = true
    def active(s: sql.DataFrame): Unit
    def deActive(): Unit
}
