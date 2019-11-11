package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxConvertSchemaJobContainer.Listener

import java.util.concurrent.TimeUnit

import com.pharbers.StreamEngine.Utils.Channel.Local.BPSLocalChannel
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamListener
import com.pharbers.StreamEngine.Utils.StreamJob.BPStreamJob
import com.pharbers.kafka.producer.PharbersKafkaProducer
import com.pharbers.kafka.schema.FileMetaData
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery

case class ConvertSchemaListener(id: String,
                                 jobId: String,
                                 spark: SparkSession,
                                 job: BPStreamJob,
                                 query: StreamingQuery,
                                 sumRow: Long) extends BPStreamListener {
	override def trigger(e: BPSEvents): Unit = {
		val cumulative = query.recentProgress.map(_.numInputRows).sum

		if (cumulative >= sumRow) {
			logger.debug("******>" + cumulative)
			// TODO 将处理好的Schema发送邮件
//				pollKafka(new FileMetaData(id, jobId, "/test/alex/" + id + "/metadata/" + "",
//					"/test/alex/" + id + "/files/" + "jobId=" + "", ""))
			job.close()
		}
	}
	
	override def active(s: DataFrame): Unit = BPSLocalChannel.registerListener(this)
	
	override def deActive(): Unit = BPSLocalChannel.unRegisterListener(this)
	
	def pollKafka(msg: FileMetaData): Unit ={
		//todo: 参数化
		val topic = ""
		val pkp = new PharbersKafkaProducer[String, FileMetaData]
		val fu = pkp.produce(topic, msg.getJobId.toString, msg)
		logger.info(fu.get(10, TimeUnit.SECONDS))
	}
}
