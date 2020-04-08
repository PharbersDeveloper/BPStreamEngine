package com.pharbers.StreamEngine.Jobs.EsJob.Listener

import java.util.UUID

import com.pharbers.StreamEngine.Jobs.EsJob.BPSEsSinkJob
import com.pharbers.StreamEngine.Utils.Job.BPSJobContainer
import com.pharbers.kafka.schema.EsSinkJobSubmit
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.SparkSession

case class EsSinkJobStartListener(id: String,
                                  spark: SparkSession,
                                  container: BPSJobContainer) {

    final val RUNNER_ID: String = id

    val process: ConsumerRecord[String, EsSinkJobSubmit] => Unit = (record: ConsumerRecord[String, EsSinkJobSubmit]) => {

        val jobId: String = UUID.randomUUID().toString
        val indexName: String = record.value().getIndexName.toString
        val metadataPath: String = record.value().getMetadataPath.toString
        val filesPath: String = record.value().getFilesPath.toString
        val checkpointLocation: String = "/jobs/" + RUNNER_ID + "/" + jobId + "/checkpoint"

        val job = BPSEsSinkJob(jobId, spark, container, Map(
            "indexName" -> indexName,
            "metadataPath" -> metadataPath,
            "filesPath" -> filesPath,
            "checkpointLocation" -> checkpointLocation
        ))
        job.open()
        job.exec()
    }

}
