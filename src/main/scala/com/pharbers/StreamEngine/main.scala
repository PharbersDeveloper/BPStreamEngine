import com.pharbers.StreamEngine.BPKafkaSession.BPKafkaSession
import com.pharbers.StreamEngine.BPSparkSession.BPSparkSession
import com.pharbers.StreamEngine.BPStreamJob.JobStrategy.KfkJobStrategy
import com.pharbers.StreamEngine.BPJobChannels.DriverChannel.DriverChannel
import com.pharbers.StreamEngine.BPJobChannels.LocalChannel.LocalChannel
import com.pharbers.StreamEngine.BPStreamJob.BPSJobContainer.BPSOssJobContainer

object main extends App {
    val spark = BPSparkSession()

    DriverChannel()
    LocalChannel()

    val job =
        BPSOssJobContainer(
            KfkJobStrategy(
                BPKafkaSession(spark)
            ),
            spark
        )
    job.open()
    job.exec()

    DriverChannel.waitForDriverDead()
}
