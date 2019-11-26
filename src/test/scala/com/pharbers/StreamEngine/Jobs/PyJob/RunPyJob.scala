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
            "jobId" -> "ee2a659d-03f5-4004-98ae-87ee65bb4b3b",
            "matedataPath" -> "hdfs:///test/alex2/ba6e2dca-629c-4df5-a1fc-1c41314ad185/metadata/",
            "filesPath" -> "hdfs:///test/alex2/ba6e2dca-629c-4df5-a1fc-1c41314ad185/files/ee2a659d-03f5-4004-98ae-87ee65bb4b3b/",
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
