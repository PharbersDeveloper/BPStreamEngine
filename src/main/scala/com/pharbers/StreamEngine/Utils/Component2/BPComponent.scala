package com.pharbers.StreamEngine.Utils.Component2

import com.pharbers.StreamEngine.Utils.Log.PhLogable
import org.apache.kafka.common.config.ConfigDef

trait BPComponent extends PhLogable{
    val componentProperty: BPComponentConfig
    final lazy val configDef: ConfigDef = createConfigDef()
    def createConfigDef(): ConfigDef
}
