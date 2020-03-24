package com.pharbers.StreamEngine.Utils.Component2
import org.apache.kafka.common.config.ConfigDef

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
}
