package com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Config.AppConfig
import org.apache.kafka.common.config.ConfigDef
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read

import scala.io.Source

trait BPSEntry {
    // TODO: this is where we create channel
    // 1. 构建其它常驻Job
    // 2. 已经创建的Container
    protected var container: Map[String, BPComponent] = Map[String, BPComponent]()
}

object BPSConcertEntry extends BPSComponentFactory with BPSEntry {
    override val componentProperty: BPComponentConfig = null
    override def createConfigDef(): ConfigDef = new ConfigDef()
    def getStrategy(name: String): AnyRef = null

    lazy val cf: BPSEntryConfig = initConfigs()
    def initConfigs(): BPSEntryConfig = {
        implicit val formats: DefaultFormats.type = DefaultFormats
        val path = AppConfig().getString(AppConfig.COMPONENT_CONFIG_PATH_KEY)
        val bs = Source.fromFile(path, "UTF-8")
        val content = bs.mkString
//        read[List[BPSComponentConfig]](content)
        read[BPSEntryConfig](content)
    }

    def queryComponentConfigWithId(id: String): Option[BPSComponentConfig] = {
        val tmp: List[BPSComponentConfig] = cf.strategies ::: cf.channels ::: cf.jobs
        tmp.find(_.id == id)
    }
    def queryComponentWithId(id: String): Option[BPComponent] = container.get(id) match {
        case Some(o) => Some(o)
        case None => queryComponentConfigWithId(id) match {
            case Some(c) => Some(getOrCreateInstance(c))
            case None => None
        }
    }

    def start(): Unit = {
        val result = startStrategies() | startChannels() | startJobs()
        if (!result) {
            // TODO: exist
            println("Entry: start job error")
            sys.exit(-1)
        }
    }

    def startStrategies(): Boolean = {
        try {
            BPSConcertEntry.queryComponentWithId("spark")
            BPSConcertEntry.queryComponentWithId("kafka")
            return true
        } catch {
            case _:Throwable => return false
        }
    }

    def startChannels(): Boolean = {
        try {
            BPSConcertEntry.queryComponentWithId("local channel")
            BPSConcertEntry.queryComponentWithId("driver channel")
            return true
        } catch {
            case _:Throwable => return false
        }
    }

    def startJobs(): Boolean = {
        return true
    }
}
