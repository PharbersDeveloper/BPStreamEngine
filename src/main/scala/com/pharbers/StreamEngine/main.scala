
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy
import com.pharbers.StreamEngine.Utils.Channel.Driver.BPSDriverChannel
import com.pharbers.StreamEngine.Utils.Channel.Local.BPSLocalChannel
import com.pharbers.StreamEngine.Jobs.OssJob.OssJobContainer.BPSOssJobContainer
import com.pharbers.StreamEngine.Utils.Session.Kafka.BPKafkaSession
import com.pharbers.StreamEngine.Utils.Session.Spark.BPSparkSession

object main extends App {

    val spark = BPSparkSession()

    BPSDriverChannel()
    BPSLocalChannel()

    val job =
        BPSOssJobContainer(
            BPSKfkJobStrategy(
                BPKafkaSession(spark)
            ),
            spark
        )
    job.open()
    job.exec()

    BPSDriverChannel.waitForDriverDead()
}
