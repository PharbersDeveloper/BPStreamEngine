package com.pharbers.StreamEngine.Common.StreamListener

import com.pharbers.StreamEngine.Common.Events
import org.apache.spark.sql

trait BPStreamListener {
    def trigger(e: Events): Unit
    def hit(e: Events): Boolean
    def active(s: sql.DataFrame): Unit
    def deActive(s: sql.DataFrame): Unit
}
