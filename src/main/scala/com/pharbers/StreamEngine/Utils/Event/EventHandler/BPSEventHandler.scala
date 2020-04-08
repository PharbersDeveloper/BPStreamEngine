package com.pharbers.StreamEngine.Utils.Event.EventHandler

import com.pharbers.StreamEngine.Utils.Job.BPStreamJob
import com.pharbers.StreamEngine.Utils.Event.BPSEvents

trait BPSEventHandler {
    def exec(job: BPStreamJob)(e: BPSEvents): Unit
    def close(): Unit
}
