package com.pharbers.StreamEngine

import com.pharbers.StreamEngine.Utils.Log.BPSLogContext
import com.pharbers.StreamEngine.Utils.Component.{ComponentConfig, ComponentContext}
import com.pharbers.StreamEngine.Utils.Component.ComponentContext.getComponentConfigLst
import com.pharbers.StreamEngine.Utils.Component2.{BPSConcertEntry, BPSComponentConfig}
import com.pharbers.StreamEngine.Utils.Config.AppConfig
import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read

import scala.io.Source

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
