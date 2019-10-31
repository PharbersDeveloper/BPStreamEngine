package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxSampleDataContainer

import java.util.UUID

import com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxSampleDataContainer.Listener.BPSSampleDataListener
import com.pharbers.StreamEngine.Utils.StreamJob.BPSJobContainer
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

object BPSSandBoxSampleDataJobContainer {
	def apply(path: String,
	          jobId: String,
	          spark: SparkSession): BPSSandBoxSampleDataJobContainer =
		new BPSSandBoxSampleDataJobContainer(path, jobId, spark)
}

class BPSSandBoxSampleDataJobContainer(path: String,
                                       jobId: String,
                                       override val spark: SparkSession) extends BPSJobContainer {
	val id = ""//UUID.randomUUID().toString
	type T = BPSKfkJobStrategy
	val strategy = null
	
	override def open(): Unit = {

		this.inputStream = Some(spark.readStream
			.schema(StructType(
				StructField("traceId", StringType) ::
				StructField("type", StringType) ::
				StructField("data", StringType) ::
				StructField("timestamp", TimestampType) :: Nil
			))
			.parquet(s"$path$jobId"))
	}
	
	override def exec(): Unit = inputStream match {
		case Some(is) =>
			val qv = s"`tmp_view_$jobId`"
			outputStream = is
				.writeStream
				.queryName(qv)
				.outputMode("update")
				.format("memory")
				.start() :: outputStream
			
			val listener = BPSSampleDataListener(spark, this, qv, jobId)
			listener.trigger(null)
			listeners = listener :: listeners

			
		case None => ???
	}
}
