package com.pharbers.StreamEngine.Utils.Event.EventHandler

import com.pharbers.StreamEngine.Utils.Job.BPStreamJob
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Log.PhLogable

trait BPSEventHandler extends PhLogable{
    def exec(job: BPStreamJob)(e: BPSEvents): Unit
    def close(): Unit
}
