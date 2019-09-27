
import java.util.UUID

import com.pharbers.StreamEngine.BPKafkaSession.BPKafkaSession
import com.pharbers.StreamEngine.BPSparkSession.BPSparkSession
import com.pharbers.StreamEngine.BPStreamJob.FileJobs.BPSOssJob
import com.pharbers.StreamEngine.BPStreamJob.JobStrategy.KfkJobStrategy
import com.pharbers.StreamEngine.BPJobChannels.DriverChannel.DriverChannel

object main extends App {
    val spark = BPSparkSession()

    DriverChannel()
    val kfk = BPKafkaSession(spark)

    val job = BPSOssJob(KfkJobStrategy(kfk), spark)
    job.open()
    job.exec()

    val jobId = UUID.randomUUID()
    val path = "hdfs://192.168.100.137:9000/test/streaming/" + jobId + "/files"

    val query = job.inputStream.get.writeStream
         .outputMode("append")
//        .outputMode("complete")
         .format("console")
//        .format("csv")
//        .option("checkpointLocation", "/test/streaming/" + jobId + "/checkpoint")
//        .option("path", "/test/streaming/" + jobId + "/files")
        .start()

    query.awaitTermination()
}
