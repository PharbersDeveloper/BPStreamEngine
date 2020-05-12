//package dcs.test
//
//import java.util.UUID
//
//import com.pharbers.StreamEngine.Jobs.ReCallJob.ReCallJobListener
//import com.pharbers.StreamEngine.Utils.Channel.Local.BPSLocalChannel
//import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor
//import org.scalatest.FunSuite
//
///** 功能描述
//  *
//  * @param args 构造参数
//  * @tparam T 构造泛型参数
//  * @author dcs
//  * @version 0.0
//  * @since 2019/12/12 14:16
//  * @note 一些值得注意的地方
//  */
//class ReCallJobListener extends FunSuite{
//    test("test ReCallJobListener"){
////        BPSLocalChannel(Map.empty)
//        BPSLocalChannel(null)
//        val jobId = UUID.randomUUID().toString
//        val runId = UUID.randomUUID().toString
//        val topic = "HiveTracebackTask"
//        val listener = ReCallJobListener(null, topic, runId, jobId)
//        listener.active(null)
//        ThreadExecutor.waitForShutdown()
//    }
//
//    test("recall"){
//        val jobId = "20191225"
//        val runId = "20191225"
//        val topic = "HiveTracebackTask"
//        val simpleDataPath = "/user/alex/jobs/86a5bf98-ee9d-40c1-9661-9590b0e6cae7/85423efb-3328-4eaa-89f5-d8208339bd9a/contents"
//        val metaDataPath = simpleDataPath.replace("contents", "metadata")
//        val listener = ReCallJobListener(null, topic, runId, jobId)
//        listener.pushPyjob(runId,metaDataPath, simpleDataPath, "", "")
//    }
//}
