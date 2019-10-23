
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy
import com.pharbers.StreamEngine.Utils.Channel.Driver.BPSDriverChannel
import com.pharbers.StreamEngine.Utils.Channel.Local.BPSLocalChannel
import com.pharbers.StreamEngine.Jobs.OssPartitionJob.OssJobContainer.BPSOssPartitionJobContainer
import com.pharbers.StreamEngine.Utils.Component.ComponentContext
import com.pharbers.StreamEngine.Utils.Config.AppConfig
import com.pharbers.StreamEngine.Utils.Session.Kafka.BPKafkaSession
import com.pharbers.StreamEngine.Utils.Session.Spark.BPSparkSession
import com.pharbers.StreamEngine.Utils.StreamJob.BPStreamJob
import collection.JavaConverters._

object main extends App {

    val spark = BPSparkSession()

    BPSDriverChannel()
    BPSLocalChannel()

    val job =
        BPSOssPartitionJobContainer(
            BPSKfkJobStrategy(
                BPKafkaSession(spark)
            ),
            spark
        )
    job.open()
    job.exec()

    BPSDriverChannel.waitForDriverDead()
}

object test extends App {

    val context = ComponentContext()
//    val jobs = AppConfig().getList(AppConfig.JOBS)
//    BPSDriverChannel()
//    BPSLocalChannel()
//    jobs.asScala.foreach(x => {
//        val job = context.getComponent[BPStreamJob](x)
//        job.open()
//        job.exec()
//    })
}
