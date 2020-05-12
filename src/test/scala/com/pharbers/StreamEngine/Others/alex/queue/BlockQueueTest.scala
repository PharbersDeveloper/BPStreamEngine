package com.pharbers.StreamEngine.Others.alex.queue

import java.util.concurrent.ArrayBlockingQueue

import com.pharbers.StreamEngine.Utils.Channel.Local.BPSLocalChannel
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPJobLocalListener
import org.scalatest.FunSuite

class BlockQueueTest extends FunSuite {
	
	val arrayBlockingQueue = new ArrayBlockingQueue[String](3)
	val localChanel: BPSLocalChannel = BPSConcertEntry.queryComponentWithId("local channel").get.asInstanceOf[BPSLocalChannel]
	
	test("test") {
		val listener = BPJobLocalListener[String](null, List(s"Test ArrayBlockingQueue"))(x => {
			println(arrayBlockingQueue.take())
		})
		listener.active(null)
		1 to 1000 foreach { n =>
			arrayBlockingQueue.put(n.toString)
			val bpsEvents = BPSEvents("", "", s"Test ArrayBlockingQueue", arrayBlockingQueue)
			localChanel.offer(bpsEvents)
		}
		Thread.sleep(1000 * 1 * 3000)
	}
}
