package com.pharbers.StreamEngine.Utils.Strategy.Queue

import java.util.concurrent.locks.Lock

// 没写好
@deprecated
object locks {
	
	class LockOps(l: Lock) {
		def apply[X](x: => X): X = {
			l.lock()
			try x finally {
				l.unlock()
			}
		}
	}
	
	implicit def LockOps(l: Lock): LockOps = new LockOps(l)
	
}
