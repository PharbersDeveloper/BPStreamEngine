package com.pharbers.StreamEngine

import com.pharbers.StreamEngine.Utils.Log.BPSLogContext
import com.pharbers.StreamEngine.Utils.Component.ComponentContext
import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor

object main extends App {
    BPSLogContext.init()
    ComponentContext.init()
    ThreadExecutor.waitForShutdown()
}




