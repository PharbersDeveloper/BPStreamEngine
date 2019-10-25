package com.pharbers.StreamEngine.Jobs.SandBoxJob

import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSJobStrategy
import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession


object BPSandBoxJob {
	def apply(id: String,
		      spark: SparkSession,
		      inputStream: Option[sql.DataFrame],
		      container: BPSJobContainer): BPSandBoxJob = new BPSandBoxJob(id, spark, inputStream, container)
}

class BPSandBoxJob (val id: String,
                    val spark: SparkSession,
                    val is: Option[sql.DataFrame],
                    val container: BPSJobContainer) extends BPStreamJob {
	type T = BPSJobStrategy
	override val strategy = null
	
	override def exec(): Unit = {
		inputStream = is
	}
	
	override def close(): Unit = {
		super.close()
		container.finishJobWithId(id)
	}
}