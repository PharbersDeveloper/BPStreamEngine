package com.pharbers.StreamEngine.Utils.Job.Status

object BPSJobStatus extends Enumeration {
	type JobStatus = Value

	val Pending: BPSJobStatus.Value = Value("pending")

	val Exec: BPSJobStatus.Value = Value("exec")

	val Success: BPSJobStatus.Value = Value("success")

	val Fail: BPSJobStatus.Value = Value("fail")

	def showAll(): Unit = this.values.foreach(println)
}
