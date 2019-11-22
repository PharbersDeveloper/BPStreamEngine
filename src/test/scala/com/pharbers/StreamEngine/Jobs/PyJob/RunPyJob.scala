package com.pharbers.StreamEngine.Jobs.PyJob

import org.json4s._
import java.util.concurrent.TimeUnit
import com.pharbers.kafka.schema.BPJob
import org.json4s.jackson.Serialization.write
import com.pharbers.kafka.producer.PharbersKafkaProducer
import com.pharbers.StreamEngine.Utils.Component.Dynamic.JobMsg
import com.pharbers.StreamEngine.Utils.Session.Spark.BPSparkSession
import com.pharbers.StreamEngine.Jobs.PyJob.PythonJobContainer.BPSPythonJobContainer

object RunPyJob extends App {

    def directStart(): Unit = {
        val job = BPSPythonJobContainer(null, BPSparkSession(), Map.empty)
        job.open()
        job.exec()
    }
//    directStart()

    def sendMsgStart(): Unit = {
        implicit val formats: DefaultFormats.type = DefaultFormats

        val jobId = "201910231514"
        val traceId = "201910231514"
        val `type` = "addList"
        val clazz = "com.pharbers.StreamEngine.Jobs.PyJob.PythonJobContainer.BPSPythonJobContainer"
        val jobs = JobMsg("pyBoxJob", "job", clazz, List("$BPSparkSession"), Nil, Nil, Map(
            "jobId" -> "2bcf28e3-4a7a-4c08-b0c7-6dddee9fb894",
            "matedataPath" -> "hdfs:///test/alex2/a6d8e117-67d7-46f7-b932-4627c6677b0f/metadata/",
            "filesPath" -> "hdfs:///test/alex2/a6d8e117-67d7-46f7-b932-4627c6677b0f/files/2bcf28e3-4a7a-4c08-b0c7-6dddee9fb894",
            "resultPath" -> "hdfs:///test/qi3/"
        ), "", "py job") :: Nil

        val jobMsg = write(jobs)
        val topic = "stream_job_submit_qi"

        val pkp = new PharbersKafkaProducer[String, BPJob]
        val bpJob = new BPJob(jobId, traceId, `type`, jobMsg)
        val fu = pkp.produce(topic, jobId, bpJob)
        println(fu.get(10, TimeUnit.SECONDS))
    }

    sendMsgStart()

}
