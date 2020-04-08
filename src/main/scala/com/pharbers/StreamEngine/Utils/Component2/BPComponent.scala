package com.pharbers.StreamEngine.Utils.Component2

import org.apache.kafka.common.config.ConfigDef

trait BPComponent {
    val componentProperty: BPComponentConfig
    final lazy val configDef: ConfigDef = createConfigDef()
    def createConfigDef(): ConfigDef
}
