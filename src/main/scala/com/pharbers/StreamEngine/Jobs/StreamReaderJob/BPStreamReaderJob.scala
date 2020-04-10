package com.pharbers.StreamEngine.Jobs.StreamReaderJob

import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Job.{BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Utils.Strategy.BPStrategyComponent
import org.apache.kafka.common.config.ConfigDef
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

class BPStreamReaderJob(
                            val id: String,
                            val spark: SparkSession,
                            val is: Option[sql.DataFrame],
                            val container: BPSJobContainer) extends BPStreamJob {

    type T = BPStrategyComponent
    override val strategy: BPStrategyComponent = null

    override def exec(): Unit = {
        // TODO: call the python code to exec
    }

    override def close(): Unit = {
        super.close()
        container.finishJobWithId(id)
    }

    override val componentProperty: Component2.BPComponentConfig = null

    override def createConfigDef(): ConfigDef = ???

    override val description: String = "stream_reader"
}
