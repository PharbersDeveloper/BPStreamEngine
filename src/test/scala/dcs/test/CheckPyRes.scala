package dcs.test

import com.amazonaws.services.s3.AmazonS3
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Strategy.s3a.BPS3aFile
import collection.JavaConverters._

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/06/03 10:01
  * @note 一些值得注意的地方
  */
object CheckS3Res extends App {
    val s3aFile: BPS3aFile = BPSConcertEntry.queryComponentWithId("s3a").get.asInstanceOf[BPS3aFile]
    val s3Client: AmazonS3 = s3aFile.getLowAipClient
    val bucketName = "ph-stream"
    var keys: List[String] = List()
    s3Client.listObjects(bucketName, "jobs/runId_5f06f30f4e81e21338d20053/BPSPyCleanJob/").getObjectSummaries.asScala
            .foreach(x => {
                val key = x.getKey
                if(key.contains("err") && !key.contains("_SUCCESS")) {

                    if(x.getSize > 0) keys = keys :+ key
                }
            })
    keys.map(x => x.split("/")(3).replace("jobId_", "")).distinct.foreach(println)
}
