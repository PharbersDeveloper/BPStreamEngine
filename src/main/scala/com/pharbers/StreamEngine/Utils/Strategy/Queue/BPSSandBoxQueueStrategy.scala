package com.pharbers.StreamEngine.Utils.Strategy.Queue

import java.util.concurrent.atomic.AtomicInteger

import com.pharbers.StreamEngine.Jobs.SandBoxJob.BPSSandBoxConvertSchemaJob
import com.pharbers.util.log.PhLogable
import org.apache.kafka.common.config.ConfigDef

import scala.collection.mutable

private object SandBoxQueue {
	val jobQueue = new mutable.Queue[BPSSandBoxConvertSchemaJob]
	val execQueueJob = new AtomicInteger(0)
}

class BPSSandBoxQueueStrategy(config: Map[String, String], inoutConfigDef: ConfigDef)
	extends Runnable with PhLogable {
	
	def push(job: BPSSandBoxConvertSchemaJob): Unit = SandBoxQueue.jobQueue.enqueue(job)
	
	def popExecJobNum(): Unit = SandBoxQueue.execQueueJob.decrementAndGet()
	
	def clean(): Unit = SandBoxQueue.jobQueue.clear()
	
	override def run(): Unit = {
		while (true) {
//			logger.info(s"execQueueJob Num =====> ${SandBoxQueue.execQueueJob.get()}")
//			logger.info(s"jobQueue Num =====> ${SandBoxQueue.jobQueue.length}")
			if (SandBoxQueue.execQueueJob.get() < config("queue").toInt && SandBoxQueue.jobQueue.nonEmpty) {
				val job = SandBoxQueue.jobQueue.dequeue()
				try {
					SandBoxQueue.execQueueJob.incrementAndGet()
					job.open()
					job.exec()
				} catch {
					case e: Exception => logger.error(e.getMessage, e); job.close()
				}
				
			}
			Thread.sleep(1 * 1000)
		}
	}
}

object BPSSandBoxQueueStrategy {
	def apply(config: Map[String, String], @transient inoutConfigDef: ConfigDef = new ConfigDef()): BPSSandBoxQueueStrategy =
		new BPSSandBoxQueueStrategy(config, inoutConfigDef)
}
