package com.pharbers.StreamEngine.Utils.Strategy.Queue

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable.ListBuffer
import com.pharbers.StreamEngine.Utils.Strategy.Queue.locks.LockOps

// 没写好
@deprecated
abstract class BlockingQueueStrategy[E] {
	protected val count = new AtomicInteger
	
	private val lock = new ReentrantLock()
	
	private val condition = lock.newCondition()
	
	def offer(elem: E): Boolean = {
		lock {
			val result = enqueue(elem)
			condition.signalAll()
			result
		}
	}
	
	def poll(): Option[E] = {
		lock {
			val result = dequeue()
			condition.signalAll()
			result
		}
	}
	
	def put(elem: E) {
		lock {
			while (!enqueue(elem)) {
				condition.await()
			}
			condition.signalAll()
		}
	}
	
	def take(): E = {
		lock {
			var result: Option[E] = None
			while (result.isEmpty) {
				result = dequeue()
				if (result.isEmpty) {
					condition.await()
				}
			}
			condition.signalAll()
			result.get
		}
	}
	
	def size(): Int = count.get()
	
	def drain(maxElements: Int = Int.MaxValue): List[E] = {
		val size = math.min(maxElements, count.get())
		
		val buf = new ListBuffer[E]()
		buf.sizeHint(size)
		
		var elem: Option[E] = None
		var i: Int = 0
		
		while ({
			if (i < size) {
				elem = poll()
				elem.isDefined
			} else {
				false
			}
		}) {
			buf.append(elem.get)
			i += 1
		}
		
		buf.result()
	}
	
	protected def enqueue(elem: E): Boolean
	
	protected def dequeue(): Option[E]
}
