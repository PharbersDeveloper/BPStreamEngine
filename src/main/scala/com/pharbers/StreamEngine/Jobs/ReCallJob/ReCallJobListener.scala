package com.pharbers.StreamEngine.Jobs.ReCallJob

import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit

import com.pharbers.StreamEngine.Utils.Channel.Local.BPSLocalChannel
import com.pharbers.StreamEngine.Utils.Component.Dynamic.JobMsg
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamListener
import com.pharbers.StreamEngine.Utils.StreamJob.BPStreamJob
import com.pharbers.kafka.consumer.PharbersKafkaConsumer
import com.pharbers.kafka.producer.PharbersKafkaProducer
import com.pharbers.kafka.schema.{BPJob, HiveTracebackTask}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.sql.DataFrame

import collection.JavaConverters._

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/12/12 12:01
  * @note 一些值得注意的地方
  */
case class ReCallJobListener(job: BPStreamJob, topic: String, runId: String, jobId: String) extends BPStreamListener {

    val consumer: KafkaConsumer[String, HiveTracebackTask] = new PharbersKafkaConsumer[String, HiveTracebackTask](Nil).getConsumer
    consumer.subscribe(List(topic).asJava)

    override def trigger(e: BPSEvents): Unit = {
        consumer.poll(Duration.ofSeconds(1)).asScala.foreach(x => {
            val simpleDataPath = x.value().getParentUrl.getSampleData.toString
            val metaDataPath = simpleDataPath.replace("contents", "metadata")
            val parentIds = x.value().getParentDatasetId.asScala.map(_.toString)
            pushPyjob(runId, metaDataPath, simpleDataPath, jobId, parentIds.mkString(","))
        })
    }

    private def pushPyjob(runId: String,
                          metadataPath: String,
                          filesPath: String,
                          parentJobId: String,
                          dsIds: String): Unit = {
        val resultPath = s"hdfs:///user/alex/jobs/$runId/${UUID.randomUUID().toString}/contents/"
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
            s"ossPyJob${UUID.randomUUID().toString}",
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
        val pkp = new PharbersKafkaProducer[String, BPJob]
        val bpJob = new BPJob(parentJobId, traceId, `type`, jobMsg)
        val fu = pkp.produce(topic, parentJobId, bpJob)
        logger.debug(fu.get(10, TimeUnit.SECONDS))
    }

    override def active(s: DataFrame): Unit = BPSLocalChannel.registerListener(this)

    override def deActive(): Unit = {
        consumer.close()
        BPSLocalChannel.unRegisterListener(this)
    }
}
