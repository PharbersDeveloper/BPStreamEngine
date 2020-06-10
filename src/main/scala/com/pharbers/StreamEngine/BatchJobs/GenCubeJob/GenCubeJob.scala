package com.pharbers.StreamEngine.BatchJobs.GenCubeJob

import java.util.UUID

import com.pharbers.StreamEngine.BatchJobs.BPBatchJob
import com.pharbers.StreamEngine.BatchJobs.GenCubeJob.Strategies.{DataCleanStrategy, GenCubeJobStrategy, WriteToEsStrategy}
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Log.PhLogable
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import org.apache.spark.sql.SparkSession

object GenCubeJob {

    def apply(sql: String, esIndex: String): GenCubeJob = {
        val jobId: String = UUID.randomUUID().toString
        val sparkSession: SparkSession = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession].spark
        new GenCubeJob(jobId, sparkSession, sql, esIndex)
    }

}

class GenCubeJob(jobId: String, sparkSession: SparkSession, sql: String, esIndex: String) extends BPBatchJob with PhLogable {

    def start = {

        if (sql.isEmpty) {
            logger.error("No sql set!")
            sys.exit()
        }
        if (esIndex.isEmpty) {
            logger.error("No es index set!")
            sys.exit()
        }

        logger.info("GenCubeJob start.")
        logger.info(s"GenCubeJob sql=($sql).")
        logger.info(s"GenCubeJob esIndex=($esIndex).")

        val reading = sparkSession.sql(sql)
        logger.info("GenCubeJob origin length =  ========>" + reading.count())

        val cleanDF = new DataCleanStrategy(sparkSession).clean(reading)
        logger.info("GenCubeJob cleanDF length =  ========>" + cleanDF.count())

        val cubeDF = new GenCubeJobStrategy(sparkSession).convert(cleanDF)
        WriteToEsStrategy().writeListDF(cubeDF, esIndex)

        logger.info("GenCubeJob done.")

    }

}
