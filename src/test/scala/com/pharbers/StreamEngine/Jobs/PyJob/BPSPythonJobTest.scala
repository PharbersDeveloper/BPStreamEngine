//package com.pharbers.StreamEngine.Jobs.PyJob
//
//import java.util.UUID
//
//import com.pharbers.StreamEngine.Utils.Channel.Local.BPSLocalChannel
////import com.pharbers.StreamEngine.Utils.GithubHelper.BPSGithubHelper
//import org.scalatest.FunSuite
//import org.mongodb.scala.bson.ObjectId
////import com.pharbers.StreamEngine.Utils.Schema.Spark.BPSParseSchema
////import com.pharbers.StreamEngine.Utils.Session.Spark.BPSparkSession
//import org.apache.spark.sql.SparkSession
//
//class BPSPythonJobTest extends FunSuite {
//    val runId: String = "runid-202003230001"
//    val containerId: String = "BPSPythonJobTest"
//
//    val pythonCodePath: String = "/Users/clock/workSpace/Python/bp-data-clean/"
//
//    val metadataPath = "/jobs/02c07385-39fa-496a-a9ac-029ed09aa79c/0db59660-056f-4d48-9ade-90b8ceaadc57/metadata"
//    val filePath = "/jobs/02c07385-39fa-496a-a9ac-029ed09aa79c/0db59660-056f-4d48-9ade-90b8ceaadc57/contents"
//
//    val spark: SparkSession = BPSparkSession(Map("app.name" -> "BPSPythonJobTest"))
////    BPSLocalChannel(Map())
//
//    // FIX 测试未通过
//    test("test run BPSPythonJob") {
//        val jobId = UUID.randomUUID().toString
//
//        // 上传 python 文件
//        BPSGithubHelper().listFile(pythonCodePath, ".py").foreach(spark.sparkContext.addFile)
//
//        // 解析 metadata
//        val metadata = BPSParseSchema.parseMetadata(metadataPath)(spark)
//        val loadSchema = BPSParseSchema.parseSchema(metadata("schema").asInstanceOf[List[_]])
//
//        // 读取输入流
//        val reading = spark.readStream
//                .schema(loadSchema)
//                .option("startingOffsets", "earliest")
//                .parquet(filePath)
//
//        val job = BPSPythonJob(jobId, spark, Some(reading), (str, m) => Unit, str => Unit, Map(
//            "noticeTopic" -> "noticeTopic",
//            "datasetId" -> new ObjectId().toString,
//            "parentsId" -> "".split(",").toList.map(_.asInstanceOf[CharSequence]),
//            "resultPath" -> s"./jobs/$runId/$containerId/",
//            "lastMetadata" -> metadata,
//            "fileSuffix" -> "csv",
//            "partition" -> "4",
//            "retryCount" -> "3"
//        ))
//
//        job.open()
//        job.exec()
//    }
//}
