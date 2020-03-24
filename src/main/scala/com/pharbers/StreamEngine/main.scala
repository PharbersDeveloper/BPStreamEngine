package com.pharbers.StreamEngine

import com.pharbers.StreamEngine.Utils.Log.BPSLogContext
import com.pharbers.StreamEngine.Utils.Component.ComponentContext
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor

object main extends App {
    BPSLogContext.init()
    ComponentContext.init()
    ThreadExecutor.waitForShutdown()
}

object main_oom extends App {
    BPSLogContext.init()
    BPSConcertEntry.queryComponentWithId("spark")
    BPSConcertEntry.queryComponentWithId("kafka")
    ThreadExecutor.waitForShutdown()
}
