package com.pharbers.StreamEngine.Common.EventHandler

import com.pharbers.StreamEngine.BPStreamJob.BPStreamJob
import com.pharbers.StreamEngine.Common.Events

trait EventHandler {
    def exec(job: BPStreamJob)(e: Events): Unit
    def close(): Unit
}
