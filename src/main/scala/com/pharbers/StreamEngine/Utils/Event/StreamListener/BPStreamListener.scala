package com.pharbers.StreamEngine.Utils.Event.StreamListener

import org.apache.spark.sql
import com.pharbers.util.log.PhLogable
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.StreamJob.BPStreamJob

trait BPStreamListener extends PhLogable {
    val job: BPStreamJob

    def trigger(e: BPSEvents): Unit

    def hit(e: BPSEvents): Boolean = true

    def active(s: sql.DataFrame): Unit

    def deActive(): Unit
}
