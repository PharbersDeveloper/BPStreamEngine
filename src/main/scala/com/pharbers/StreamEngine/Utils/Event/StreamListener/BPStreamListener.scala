package com.pharbers.StreamEngine.Utils.Event.StreamListener

import com.pharbers.StreamEngine.Utils.StreamJob.BPStreamJob
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import org.apache.spark.sql

trait BPStreamListener {
    val job: BPStreamJob

    def trigger(e: BPSEvents): Unit
    def hit(e: BPSEvents): Boolean = true
    def active(s: sql.DataFrame): Unit
    def deActive(): Unit
}
