import com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxConvertSchemaJob.BPSSandBoxConvertSchemaJob
import com.pharbers.StreamEngine.Utils.Component.ComponentContext
import com.pharbers.StreamEngine.Utils.Session.Spark.BPSparkSession
import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor

object main extends App {
    
    val context = ComponentContext()
	
//	val spark = BPSparkSession()
//	val job = BPSSandBoxConvertSchemaJob("001", "/test/alex/test001/metadata",
//		"/test/alex/test001/files/jobId=",
//		"da0fb-c055-4d27-9d1a-fc9890", spark)
//	job.open()
//	job.exec()
    ThreadExecutor.waitForShutdown()

}
