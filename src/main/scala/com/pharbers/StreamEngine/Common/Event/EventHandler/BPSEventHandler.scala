package com.pharbers.StreamEngine.Common.Event.EventHandler

import com.pharbers.StreamEngine.BPStreamJob.BPStreamJob
import com.pharbers.StreamEngine.Common.Event.BPSEvents

trait BPSEventHandler {
    def exec(job: BPStreamJob)(e: BPSEvents): Unit
    def close(): Unit
}
