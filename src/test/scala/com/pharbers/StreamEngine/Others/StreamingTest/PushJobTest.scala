package com.pharbers.StreamEngine.Others.StreamingTest

import java.io.File
import java.util.UUID
import java.util.concurrent.TimeUnit

import com.pharbers.StreamEngine.Utils.Component.Dynamic.JobMsg
import com.pharbers.StreamEngine.Utils.Kafka.ProducerSingleton
import com.pharbers.kafka.producer.PharbersKafkaProducer
import com.pharbers.kafka.schema.BPJob
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.scalatest.FunSuite

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/10/23 15:01
  * @note 一些值得注意的地方
  */
class PushJobTest extends FunSuite {
    test("push job") {
        import org.json4s._
        import org.json4s.jackson.Serialization.write
        implicit val formats: DefaultFormats.type = DefaultFormats
        val jobId = "201910231514"
        val traceId = "201910231514"
        val `type` = "add"
        val jobs =
            JobMsg("ossStreamJob", "job", "com.pharbers.StreamEngine.Jobs.OssPartitionJob.OssJobContainer.BPSOssPartitionJobContainer", List("$BPSKfkJobStrategy", "$BPSparkSession"), Nil, Nil, Map.empty, "", "oss job") ::
                JobMsg("sandBoxJob", "job", "com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxJobContainer.BPSSandBoxJobContainer", List("$BPSparkSession"), Nil, Nil, Map.empty, "", "sandbox job") ::
                Nil
        //        val jobMsg = write(JobMsg("testJob", "job", "com.pharbers.StreamEngine.Jobs.OssJob.DynamicJobDemo",
        //            List("$BPSparkSession", "$BPSKfkJobStrategy", "demo"), Nil, Nil, Map.empty, "", "test job"))
        //        val jobMsg = write(JobMsg("testListener", "listener", "com.pharbers.StreamEngine.Jobs.OssJob.DynamicListenerDemo",
        //            Nil, List("testJob"), List("id", "this"), Map.empty, "", "test listener"))
        //        val jobMsg = write(JobMsg("testListener2", "listener", "com.pharbers.StreamEngine.Jobs.OssJob.DynamicListenerDemo",
        //            List("another listener"), List("testJob"), List("this"), Map.empty, "", "test listener"))
        val jobMsg = write(JobMsg("ossStreamJob", "job", "com.pharbers.StreamEngine.Jobs.OssPartitionJob.OssJobContainer.BPSOssPartitionJobContainer",
            List("$BPSKfkJobStrategy", "$BPSparkSession"), Nil, Nil, Map.empty, "", "test job"))
        val topic = "stream_job_submit"
    
        val pkp = new PharbersKafkaProducer[String, BPJob]
        val bpJob = new BPJob(jobId, traceId, `type`, jobMsg)
        val fu = pkp.produce(topic, jobId, bpJob)
        println(fu.get(10, TimeUnit.SECONDS))
    }
    
    test("push jobs") {
        import org.json4s._
        import org.json4s.jackson.Serialization.write
        implicit val formats: DefaultFormats.type = DefaultFormats
        val jobId = "201910231514"
        val traceId = "201910231514"
        val `type` = "addList"
        val jobs =
            JobMsg("ossStreamJob", "job", "com.pharbers.StreamEngine.Jobs.OssPartitionJob.OssJobContainer.BPSOssPartitionJobContainer", List("$BPSKfkJobStrategy", "$BPSparkSession"), Nil, Nil, Map.empty, "", "oss job") ::
                JobMsg("sandBoxJob", "job", "com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxJobContainer.BPSSandBoxJobContainer", List("$BPSparkSession"), Nil, Nil, Map.empty, "", "sandbox job") ::
                //                JobMsg("pyBoxJob", "job", "com.pharbers.StreamEngine.Jobs.PyJob.PythonJobContainer.BPSPythonJobContainer", List("$BPSparkSession"), Nil, Nil, Map(
                //                    "jobId" -> "20796d42-c177-4838-9a20-79bfba60d036",
                //                    "matedataPath" -> "/test/alex2/b1b6875c-a590-4dfd-9aa7-f852596266ef/metadata/",
                //                    "filesPath" -> "/test/alex2/b1b6875c-a590-4dfd-9aa7-f852596266ef/files/20796d42-c177-4838-9a20-79bfba60d036",
                //                    "resultPath" -> "hdfs:///test/dcs/testPy2"
                //                ), "", "py job") ::
                Nil
        
        val jobMsg = write(jobs)
        val topic = "stream_job_submit"
        
        val pkp = new PharbersKafkaProducer[String, BPJob]
        val bpJob = new BPJob(jobId, traceId, `type`, jobMsg)
        val fu = pkp.produce(topic, jobId, bpJob)
        println(fu.get(10, TimeUnit.SECONDS))
    }
    
    test("stop job") {
        import org.json4s._
        implicit val formats: DefaultFormats.type = DefaultFormats
        val jobId = "201910231514"
        val traceId = "201910231514"
        val `type` = "stop"
        val jobMsg = "SandBoxJobContainer_6260762d-5c7a-495f-be09-70128d8a440b"
        
        val topic = "stream_job_submit"
        
        val pkp = new PharbersKafkaProducer[String, BPJob]
        val bpJob = new BPJob(jobId, traceId, `type`, jobMsg)
        val fu = pkp.produce(topic, jobId, bpJob)
        println(fu.get(10, TimeUnit.SECONDS))
    }
    
    test("test py job") {
        pushPyjob(
            "004",
            s"/user/alex/jobs/49324e09-d7f9-4455-935a-b33c0d27d59e/c912aa89-595c-4479-a3c3-7c6af8e2425a/metadata",
            s"/user/alex/jobs/49324e09-d7f9-4455-935a-b33c0d27d59e/c912aa89-595c-4479-a3c3-7c6af8e2425a/contents",
            UUID.randomUUID().toString,
            ("001" :: Nil).mkString(",")
        )
    }
    
    // TODO：因还未曾与老齐对接口，暂时放到这里
    private def pushPyjob(runId: String,
                          metadataPath: String,
                          filesPath: String,
                          parentJobId: String,
                          dsIds: String): Unit = {
        //		val resultPath = s"hdfs://jobs/$runId/"
        val resultPath = s"hdfs:///user/alex/jobs/$runId"
        
        import org.json4s._
        import org.json4s.jackson.Serialization.write
        implicit val formats: DefaultFormats.type = DefaultFormats
        val traceId = ""
        val `type` = "add"
        val jobConfig = Map(
            "jobId" -> parentJobId,
            "parentsOId" -> dsIds,
            "metadataPath" -> metadataPath,
            "filesPath" -> filesPath,
            "resultPath" -> resultPath
        )
        val job = JobMsg(
            s"ossPyJob$parentJobId",
            "job",
            "com.pharbers.StreamEngine.Jobs.PyJob.PythonJobContainer.BPSPythonJobContainer",
            List("$BPSparkSession"),
            Nil,
            Nil,
            jobConfig,
            "",
            "temp job")
        val jobMsg = write(job)
        val topic = "stream_job_submit"
        val bpJob = new BPJob(parentJobId, traceId, `type`, jobMsg)
        val fu = ProducerSingleton.getIns.produce(topic, parentJobId, bpJob)
        println(fu.get(10, TimeUnit.SECONDS))
        
    }
}
