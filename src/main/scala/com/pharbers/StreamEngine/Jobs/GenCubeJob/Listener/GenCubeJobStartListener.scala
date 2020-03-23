package com.pharbers.StreamEngine.Jobs.GenCubeJob.Listener

import java.util.UUID

import com.pharbers.StreamEngine.Jobs.GenCubeJob.BPSGenCubeJob
import com.pharbers.StreamEngine.Utils.StreamJob.BPSJobContainer
import com.pharbers.kafka.schema.GenCubeJobSubmit
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.SparkSession

case class GenCubeJobStartListener(id: String,
                                   spark: SparkSession,
                                   container: BPSJobContainer) {

    final val RUNNER_ID: String = id

    val process: ConsumerRecord[String, GenCubeJobSubmit] => Unit = (record: ConsumerRecord[String, GenCubeJobSubmit]) => {

        val jobId: String = UUID.randomUUID().toString
        val inputDataType: String = record.value().getInputDataType.toString
        val inputPath: String = record.value().getInputPath.toString
        val outputDataType: String = record.value().getOutputDataType.toString
        val outputPath: String = record.value().getOutputPath.toString
        val strategy: String = record.value().getStrategy.toString
        val checkpointLocation: String = "/jobs/" + RUNNER_ID + "/" + jobId + "/checkpoint"

        val job = BPSGenCubeJob(jobId, spark, container, Map(
            BPSGenCubeJob.INPUT_DATA_TYPE_KEY -> inputDataType,
            BPSGenCubeJob.INPUT_PATH_KEY -> inputPath,
            BPSGenCubeJob.OUTPUT_DATA_TYPE_KEY -> outputDataType,
            BPSGenCubeJob.OUTPUT_PATH_KEY -> outputPath,
            BPSGenCubeJob.STRATEGY_CMD_KEY -> strategy,
            BPSGenCubeJob.CHECKPOINT_LOCATION_KEY -> checkpointLocation
        ))
        job.open()
        job.exec()
    }

}
