package com.pharbers.StreamEngine.Jobs.PyJob.PythonJobContainer

import java.util.UUID
import org.apache.spark.sql.SparkSession
import com.pharbers.StreamEngine.Jobs.PyJob.BPSPythonJob
import com.pharbers.StreamEngine.Utils.StreamJob.BPSJobContainer
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy

object BPSPythonJobContainer {
    def apply(strategy: BPSKfkJobStrategy, spark: SparkSession): BPSPythonJobContainer =
        new BPSPythonJobContainer(strategy, spark)
}

class BPSPythonJobContainer(override val strategy: BPSKfkJobStrategy,
                            override val spark: SparkSession) extends BPSJobContainer with Serializable {
    type T = BPSKfkJobStrategy

    val id: String = "abc059" //UUID.randomUUID().toString

    override def open(): Unit = {
        lazy val loadSchema: StructType = new StructType()
                .add("jobId", StringType)
                .add("traceId", StringType)
                .add("type", StringType)
                .add("data", StringType)
                .add("timestamp", TimestampType)

//        val reading = spark.readStream.format("socket").option("host", "192.168.100.118").option("port", 9999).load

        val reading = spark.readStream
                .schema(loadSchema)
                .option("startingOffsets", "earliest")
                .parquet("hdfs:///test/alex/test000/files/jobId=1aed8-53d5-48f3-b7dd-780be0")

        inputStream = Some(reading)
    }

    override def exec(): Unit = inputStream match {
        case Some(_) =>
            val job = BPSPythonJob(id, spark, inputStream, this)
            spark.sparkContext.addFile("./hello_world.py")
            job.open()
            job.exec()
        case None => ???
    }
}
