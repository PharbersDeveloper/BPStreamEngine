package com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Config.AppConfig
import org.apache.kafka.common.config.ConfigDef
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read
import org.mongodb.scala.bson.ObjectId

import scala.io.Source

trait BPSEntry {
    // TODO: this is where we create channel
    // 1. 构建其它常驻Job
    // 2. 已经创建的Container
    protected var container: Map[String, BPComponent] = Map[String, BPComponent]()
}

object BPSConcertEntry extends BPSComponentFactory with BPSEntry {
    lazy val runner_id: String = new ObjectId().toString
    override val componentProperty: BPComponentConfig = null
    override def createConfigDef(): ConfigDef = new ConfigDef()
    lazy val useLazyConstruction = false

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
//        val result = startJobs()
        if (!result) {
            // TODO: exist
            println("Entry: start job error")
            sys.exit(-1)
        }
    }

    def startStrategies(): Boolean = {
        try {
            if (!useLazyConstruction)
                cf.strategies.foreach(x => queryComponentWithId(x.id))
            return true
        } catch {
            case ex: Exception =>
                ex.printStackTrace()
                return false
        }
    }

    def startChannels(): Boolean = {
        try {
            if (!useLazyConstruction)
                cf.channels.foreach(x => queryComponentWithId(x.id))
            return true
        } catch {
            case ex: Exception =>
                ex.printStackTrace()
                return false
        }
    }

    def startJobs(): Boolean = {
        try {
            if (!useLazyConstruction)
                cf.jobs.foreach(x => queryComponentWithId(x.id))
            return true
        } catch {
            case ex: Exception =>
                ex.printStackTrace()
                return false
        }
    }
}
