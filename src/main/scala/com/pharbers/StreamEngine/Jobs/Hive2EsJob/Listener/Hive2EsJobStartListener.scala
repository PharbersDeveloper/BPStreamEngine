package com.pharbers.StreamEngine.Jobs.Hive2EsJob.Listener

import java.util.UUID

import com.pharbers.StreamEngine.Jobs.Hive2EsJob.BPSHive2EsJob
import com.pharbers.StreamEngine.Utils.StreamJob.BPSJobContainer
import com.pharbers.kafka.schema.Hive2EsJobSubmit
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.SparkSession

case class Hive2EsJobStartListener(id: String,
                                   spark: SparkSession,
                                   container: BPSJobContainer) {

    final val RUNNER_ID: String = id

    val process: ConsumerRecord[String, Hive2EsJobSubmit] => Unit = (record: ConsumerRecord[String, Hive2EsJobSubmit]) => {

        val jobId: String = UUID.randomUUID().toString
        val indexName: String = record.value().getIndexName.toString
        val sqlString: String = record.value().getSql.toString
        val checkpointLocation: String = "/jobs/" + RUNNER_ID + "/" + jobId + "/checkpoint"

        val job = BPSHive2EsJob(jobId, spark, container, Map(
            BPSHive2EsJob.INDEX_NAME_KEY -> indexName,
            BPSHive2EsJob.SQL_STRING_KEY -> sqlString,
            BPSHive2EsJob.CHECKPOINT_LOCATION_KEY -> checkpointLocation
        ))
        job.open()
        job.exec()
    }

}
