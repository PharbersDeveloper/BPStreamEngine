package com.pharbers.StreamEngine.BatchJobs.GenCubeJob

import java.util.UUID

import com.pharbers.StreamEngine.BatchJobs.BPBatchJob
import com.pharbers.StreamEngine.BatchJobs.CommonStrategy.WriteStrategy
import com.pharbers.StreamEngine.BatchJobs.CommonStrategy.WriteStrategy._
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Log.PhLogable
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import org.apache.spark.sql.SparkSession

object GenCubeJob {

    def apply(sql: String): GenCubeJob = {
        val jobId: String = UUID.randomUUID().toString
        val sparkSession: SparkSession = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession].spark
        new GenCubeJob(jobId, sql, sparkSession)
    }

    def apply(jobId: String, sql: String): GenCubeJob = {
        val sparkSession: SparkSession = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession].spark
        new GenCubeJob(jobId, sql, sparkSession)
    }

}

class GenCubeJob(jobId: String, sql: String, sparkSession: SparkSession) extends BPBatchJob with PhLogable {


    override val id: String = jobId

    def start = {

        if (sql.isEmpty) {
            logger.error("No sql set!")
            sys.exit()
        }

        logger.info("GenCubeJob start.")
        logger.info(s"GenCubeJob sql=($sql).")

        val reading = sparkSession.sql(sql)
        logger.info("GenCubeJob origin length =  ========>" + reading.count())



//        val cleanDF = new DataCleanStrategy(sparkSession).clean(reading)
//        logger.info("GenCubeJob cleanDF length =  ========>" + cleanDF.count())
//
//        val cubeDF = new GenCubeJobStrategy(sparkSession).convert(cleanDF)

        WriteStrategy(PARQUET_SOURCE_TYPE).writeDF(reading, getJobStoragePath)

        logger.info("GenCubeJob done.")

    }

}
