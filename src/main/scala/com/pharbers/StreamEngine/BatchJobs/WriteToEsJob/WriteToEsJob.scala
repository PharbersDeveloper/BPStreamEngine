package com.pharbers.StreamEngine.BatchJobs.WriteToEsJob

import java.util.UUID

import com.pharbers.StreamEngine.BatchJobs.BPBatchJob
import com.pharbers.StreamEngine.BatchJobs.CommonStrategy.WriteStrategy
import com.pharbers.StreamEngine.BatchJobs.CommonStrategy.WriteStrategy._
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Log.PhLogable
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import org.apache.spark.sql.SparkSession

object WriteToEsJob {

    def apply(target: String): WriteToEsJob = {
        val jobId: String = UUID.randomUUID().toString
        val sparkSession: SparkSession = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession].spark
        new WriteToEsJob(jobId, target, sparkSession)
    }

    def apply(jobId: String, target: String): WriteToEsJob = {
        val sparkSession: SparkSession = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession].spark
        new WriteToEsJob(jobId, target, sparkSession)
    }

}

class WriteToEsJob(jobId: String, target: String, sparkSession: SparkSession) extends BPBatchJob with PhLogable {


    override val id: String = jobId

    def start = {

        if (target.isEmpty) {
            logger.error("No target set!")
            sys.exit()
        }

        logger.info("WriteToEsJob start.")
        logger.info(s"WriteToEsJob target=($target).")

        val reading = sparkSession.read.load(getJobStoragePath)

        WriteStrategy(ES_SOURCE_TYPE).writeDF(reading, target)

        logger.info("WriteToEsJob done.")

    }

}
