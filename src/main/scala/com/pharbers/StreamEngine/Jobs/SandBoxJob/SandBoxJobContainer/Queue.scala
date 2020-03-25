package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxJobContainer

import java.util.concurrent.atomic.AtomicInteger

import com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxConvertSchemaJob.BPSSandBoxConvertSchemaJob
import com.pharbers.util.log.PhLogable
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

case class Queue(spark: SparkSession) extends Runnable with PhLogable {
	val jobQueue = new mutable.Queue[Map[String, String]]
	val maxQueueJob = 3
	val execQueueJob = new AtomicInteger(0)
	var reading: Option[org.apache.spark.sql.DataFrame] = None
	override def run(): Unit = {
		while (true) {
			logger.info(s"jobQueue Num =====> ${jobQueue.length}")
			if (execQueueJob.get() <= maxQueueJob && jobQueue.nonEmpty) {
				val parm = jobQueue.dequeue()
				execQueueJob.incrementAndGet()
				val convertJob: BPSSandBoxConvertSchemaJob =
					BPSSandBoxConvertSchemaJob(
						parm("runId"),
						parm,
						spark,
						reading,
						execQueueJob)
				convertJob.open()
				convertJob.exec()
			}
			Thread.sleep(1 * 1000)
		}
	}
}
