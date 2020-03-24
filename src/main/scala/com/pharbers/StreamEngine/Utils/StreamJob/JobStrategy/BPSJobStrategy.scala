package com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy

import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Component2.BPComponent

trait BPSJobStrategy extends BPComponent {
//    override val componentProperty: Component2.BPConfig = null
    def getTopic: String
    def getSchema: org.apache.spark.sql.types.DataType
}
