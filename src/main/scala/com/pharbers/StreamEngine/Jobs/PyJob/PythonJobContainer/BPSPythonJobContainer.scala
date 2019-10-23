package com.pharbers.StreamEngine.Jobs.PyJob.PythonJobContainer

import java.util.UUID
import org.apache.spark.sql.SparkSession
import com.pharbers.StreamEngine.Jobs.PyJob.BPSPythonJob
import com.pharbers.StreamEngine.Utils.StreamJob.BPSJobContainer
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy

object BPSPythonJobContainer {
    def apply(strategy: BPSKfkJobStrategy, spark: SparkSession): BPSPythonJobContainer =
        new BPSPythonJobContainer(strategy, spark)
}

class BPSPythonJobContainer(override val strategy: BPSKfkJobStrategy,
                            override val spark: SparkSession) extends BPSJobContainer {
    val id: String = UUID.randomUUID().toString
    type T = BPSKfkJobStrategy

    override def open(): Unit = {
        val reading = spark.read
                .parquet("hdfs:///test/alex/test000/files/jobId=1aed8-53d5-48f3-b7dd-780be0")

        inputStream = Some(reading)
//                .selectExpr(
//                    """deserialize(value) AS value""",
//                    "timestamp"
//                ).toDF()
//                .withWatermark("timestamp", "24 hours")
//                .select(
//                    from_json($"value", strategy.getSchema).as("data"), col("timestamp")
//                ).select("data.*", "timestamp"))
    }

    override def exec(): Unit = inputStream match {
        case Some(_) =>
            val job = BPSPythonJob(id, spark, inputStream, this)
            job.exec()
        case None => ???
    }
}
