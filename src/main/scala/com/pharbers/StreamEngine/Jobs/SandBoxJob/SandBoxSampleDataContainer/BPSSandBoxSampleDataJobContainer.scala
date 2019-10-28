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
	val id = UUID.randomUUID().toString
	type T = BPSKfkJobStrategy
	val strategy = null
	
	override def open(): Unit = {
		val loadSchema =
			StructType(
					StructField("traceId", StringType) ::
					StructField("type", StringType) ::
					StructField("data", StringType) ::
					StructField("timestamp", TimestampType) :: Nil
			)

		this.inputStream = Some(spark.readStream
			.schema(loadSchema)
			.parquet(s"$path$jobId"))
	}
	
	override def exec(): Unit = inputStream match {
		case Some(is) =>
			val listener = BPSSampleDataListener(spark, this)
			listener.active(is)
			listeners = listener :: listeners
			
		case None => ???
	}
}
