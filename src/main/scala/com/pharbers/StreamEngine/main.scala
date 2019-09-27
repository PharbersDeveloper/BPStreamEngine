
import java.util.UUID

import org.apache.spark.sql.{ForeachWriter, Row}
import com.pharbers.StreamEngine.BPKafkaSession.BPKafkaSession
import com.pharbers.StreamEngine.BPSparkSession.BPSparkSession
import com.pharbers.StreamEngine.BPStreamJob.FileJobs.BPSOssJob
import com.pharbers.StreamEngine.BPStreamJob.JobStrategy.KfkJobStrategy
import com.pharbers.StreamEngine.Common.Events
import com.pharbers.StreamEngine.DriverChannel.DriverChannel
import com.pharbers.StreamEngine.WorkerChannel.WorkerChannel
import org.json4s._
import org.json4s.jackson.Serialization.write

object main extends App {
    val spark = BPSparkSession()
    import spark.implicits._

    DriverChannel()
    val kfk = BPKafkaSession(spark)

    val job = BPSOssJob(KfkJobStrategy(kfk), spark)
    job.open()

    val selectDf = job.inputStream.get
    val dataDf = selectDf.filter($"jobId" === "test09251043")

    val dataSchemaDf = selectDf.filter($"type" === "SandBox-Schema").writeStream
    .foreach(
        new ForeachWriter[Row] {

            var channel: Option[WorkerChannel] = None

            def open(partitionId: Long, version: Long): Boolean = {
                if (channel.isEmpty) channel = Some(WorkerChannel())
                true
            }

            def process(value: Row) : Unit = {

                implicit val formats = DefaultFormats

                val event = Events(
                    value.getAs[String]("jobId"),
                    value.getAs[String]("traceId"),
                    value.getAs[String]("type"),
                    value.getAs[String]("data")
                )

                channel.get.pushMessage(write(event))
            }

            def close(errorOrNull: scala.Throwable): Unit = {}//channel.get.close()
        }
    ).start()


    val dataEndDf = selectDf.filter($"type" === "SandBox-Length").writeStream
        .foreach(
            new ForeachWriter[Row] {

                var channel: Option[WorkerChannel] = None

                def open(partitionId: Long, version: Long): Boolean = {
                    if (channel.isEmpty) channel = Some(WorkerChannel())
                    true
                }

                def process(value: Row) : Unit = {

                    implicit val formats = DefaultFormats

                    val event = Events(
                        value.getAs[String]("jobId"),
                        value.getAs[String]("traceId"),
                        value.getAs[String]("type"),
                        value.getAs[String]("data")
                    )

                    channel.get.pushMessage(write(event))
                }

                def close(errorOrNull: scala.Throwable): Unit = {}//channel.get.close()
            }
        ).start()

    val jobId = UUID.randomUUID()
    val path = "hdfs://192.168.100.137:9000/test/streaming/" + jobId + "/files"

    val query = selectDf.writeStream
         .outputMode("append")
//        .outputMode("complete")
         .format("console")
//        .format("csv")
//        .option("checkpointLocation", "/test/streaming/" + jobId + "/checkpoint")
//        .option("path", "/test/streaming/" + jobId + "/files")
        .start()

    query.awaitTermination()
}
