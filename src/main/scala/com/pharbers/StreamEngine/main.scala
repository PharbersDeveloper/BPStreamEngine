package com.pharbers.StreamEngine

import com.pharbers.StreamEngine.Utils.Strategy.Log.BPSLogContext
import com.pharbers.StreamEngine.Utils.Component.ComponentContext
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor

object main extends App {
    BPSLogContext(null)
    ComponentContext.init()
    ThreadExecutor.waitForShutdown()
}

object main_oom extends App {
    BPSConcertEntry.start()
    ThreadExecutor.waitForShutdown()
}
