
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy
import com.pharbers.StreamEngine.Utils.Channel.Driver.BPSDriverChannel
import com.pharbers.StreamEngine.Utils.Channel.Local.BPSLocalChannel
import com.pharbers.StreamEngine.Jobs.OssPartitionJob.OssJobContainer.BPSOssPartitionJobContainer
import com.pharbers.StreamEngine.Jobs.PyJob.PythonJobContainer.BPSPythonJobContainer
import com.pharbers.StreamEngine.Utils.Component.ComponentContext
import com.pharbers.StreamEngine.Utils.Config.AppConfig
import com.pharbers.StreamEngine.Utils.Session.Kafka.BPKafkaSession
import com.pharbers.StreamEngine.Utils.Session.Spark.BPSparkSession
import com.pharbers.StreamEngine.Utils.StreamJob.BPStreamJob

object main extends App {

    val spark = BPSparkSession()

    BPSDriverChannel()
    BPSLocalChannel()

    val job =
        BPSPythonJobContainer(
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

    import collection.JavaConverters._

    val context = ComponentContext()
    val jobs = AppConfig().getList(AppConfig.JOBS)
    BPSDriverChannel()
    BPSLocalChannel()
    jobs.asScala.foreach(x => {
        val job = context.getComponent[BPStreamJob](x)
        job.open()
        job.exec()
    })
}

object a_test extends App {
    val spark = BPSparkSession()

    BPSDriverChannel()
    BPSLocalChannel()

    val job = BPStreamReaderJobContainer(spark)
    job.open()
    job.exec()

    BPSDriverChannel.waitForDriverDead()
}