package com.pharbers.StreamEngine

import com.pharbers.StreamEngine.Utils.Log.BPSLogContext
import com.pharbers.StreamEngine.Utils.Component.{ComponentConfig, ComponentContext}
import com.pharbers.StreamEngine.Utils.Component.ComponentContext.getComponentConfigLst
import com.pharbers.StreamEngine.Utils.Component2.{BPSConcertEntry, BPStgComponentConfig}
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
    implicit val formats: DefaultFormats.type = DefaultFormats
    val path = AppConfig().getString(AppConfig.COMPONENT_CONFIG_PATH_KEY)
    val bs = Source.fromFile(path, "UTF-8")
    val content = bs.mkString
    val cf = read[List[BPStgComponentConfig]](content)
    print(cf)
    BPSConcertEntry.getOrCreateInstance(cf.head)
//    ThreadExecutor.waitForShutdown()
}
