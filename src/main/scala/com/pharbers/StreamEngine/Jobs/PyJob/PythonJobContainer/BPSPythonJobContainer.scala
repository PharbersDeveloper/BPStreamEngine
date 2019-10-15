package com.pharbers.StreamEngine.Jobs.PyJob.PythonJobContainer

import java.util.UUID

import com.pharbers.StreamEngine.Utils.StreamJob.BPSJobContainer
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy
import org.apache.spark.sql.SparkSession

object BPSPythonJobContainer {
    def apply(): BPSPythonJobContainer = new BPSPythonJobContainer()
}

class BPSPythonJobContainer(
                               override val strategy: BPSKfkJobStrategy,
                               override val spark: SparkSession) extends BPSJobContainer {
    val id = UUID.randomUUID().toString
    type T = BPSKfkJobStrategy

    import spark.implicits._


}
