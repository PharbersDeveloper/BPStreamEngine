package com.pharbers.StreamEngine.Others.alex.queue

import org.scalatest.FunSuite
import java.util.Date
import java.util.concurrent.{ArrayBlockingQueue, ThreadPoolExecutor, TimeUnit}

import scala.concurrent.{ExecutionContext, Future}

class BlockQueueTest extends FunSuite {
	
	val NumberOfWorkers = 3
	val QueueCapacity = 200
	
	implicit val ec = ExecutionContext.fromExecutor(new ThreadPoolExecutor(
		NumberOfWorkers, NumberOfWorkers,
		0L, TimeUnit.SECONDS,
		new ArrayBlockingQueue[Runnable](QueueCapacity) {
			override def offer(e: Runnable) = {
				put(e);
				true
			}
		}
	))
	var counter = 0
	
	def processImage(): Future[Unit] = {
		Future {
			println("requesting " + new Date)
			Thread.sleep(10000)
			println("finishing " + new Date + "\n")
		}
	}
	
	test("blocking") {
		while (counter < 100) {
			
			processImage()
			Thread.sleep(100)
			counter = counter + 1
		}
	}
}
