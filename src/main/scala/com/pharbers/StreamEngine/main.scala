
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy
import com.pharbers.StreamEngine.Utils.Channel.Driver.BPSDriverChannel
import com.pharbers.StreamEngine.Utils.Channel.Local.BPSLocalChannel
import com.pharbers.StreamEngine.Jobs.OssPartitionJob.OssJobContainer.BPSOssPartitionJobContainer
import com.pharbers.StreamEngine.Jobs.StreamReaderJob.StreamReaderJobContainer.BPStreamReaderJobContainer
import com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxJobContainer.BPSSandBoxJobContainer
import com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxSampleDataContainer.BPSSandBoxSampleDataJobContainer
import com.pharbers.StreamEngine.Utils.Component.ComponentContext
import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor

object main extends App {
    
    val context = ComponentContext()
    ThreadExecutor.waitForShutdown()

//    val spark = BPSparkSession()
//
//    BPSDriverChannel()
//    BPSLocalChannel()

//    val job =
//        BPSOssPartitionJobContainer(
//            BPSKfkJobStrategy(
//                BPKafkaSession(spark)
//            ),
//            spark
//        )
//    job.open()
//    job.exec()

// TODO 整体SandBox初始化
//    val SandBoxJob = BPSSandBoxJobContainer(spark)
//    SandBoxJob.exec()
//
//
//    BPSDriverChannel.waitForDriverDead()
}
